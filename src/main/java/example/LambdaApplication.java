package example;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;



public class LambdaApplication implements RequestHandler<SNSEvent, String> {

	private static final String DYNAMO_TABLE_NAME = "assignment-email-table";

	private static final String GCS_BUCKET_NAME = "csye6225-demo2";
	
	private boolean urlDownloadFlag = false; 

	@Override
	public String handleRequest(SNSEvent input, Context context) {

		try {

			for (SNSEvent.SNSRecord record : input.getRecords()) {
				String message = record.getSNS().getMessage();

				System.out.println("Received SNS message: " + message);

				
				String[] parts = message.split(";");
				String username = extractValue(parts, "username");
				String assignmentId = extractValue(parts, "assignmentId");
				String assignmentName = extractValue(parts, "assignmentName");
				String submissionId = extractValue(parts, "submissionId");
				String submissionUrl = extractValue(parts, "submissionUrl");
				String submissionNumber = extractValue(parts, "submissionNo");

				InputStream artifactStream = downloadArtifact(submissionUrl);

				String objectName = username + "/" + assignmentName + "/" + submissionNumber + "/" + submissionId + ".zip";
				if(urlDownloadFlag==true) {
					storeInGoogleCloudStorage(objectName, artifactStream);
					updateDynamoDB(username, assignmentName, submissionId, "SUCCESSFUL");
					sendEmail(assignmentName, username, submissionUrl, submissionNumber, objectName);	
				}
				else {
					updateDynamoDB(username, assignmentName, submissionId, "NOT SUCCESSFUL");
					sendEmailForInvalid(assignmentName, username, submissionUrl, submissionNumber);
				}
				

			}

		} catch (Exception e) {
			System.out.println("Exception in LambdaApplication: " + e.getMessage());
		}

		System.out.println("Lambda function ended");

		return "Hello, World!";
	}

	private String extractValue(String[] parts, String key) {
		for (String part : parts) {
			if (part.startsWith(key + "=")) {
				return part.substring(key.length() + 1);
			}
		}
		return null; 
	}

	private void sendEmail(String assignmentName, String username, String submissionUrl, String submissionNumber, String objectName) {
    	try {
    		 String subject = "Assignment Submission Status - " + assignmentName;
    		 String encodedUrl = URLEncoder.encode(submissionUrl, StandardCharsets.UTF_8.toString());

    		 String bodyText = "Dear " + username + ",\n\n"
    	                + "Your assignment " + assignmentName + " with attempt number " + submissionNumber + " has been successfully submitted.\n\n"
    	                + "Submission URL: " + encodedUrl + "\n"
    	                + "File Path: " + objectName + "\n\n"
    	                + "Thank you for your submission!\n\n"
    	                + "Best regards,\nThe Assignment Team";

    	        String bodyHtml = "<h2>Dear " + username + ",</h2>"
    	                + "<p>Your assignment " + assignmentName + " with attempt number " + submissionNumber + " has been successfully submitted.</p>"
    	                + "<p>Submission URL: <a href='" + encodedUrl + "'>" + encodedUrl + "</a></p>"
    	                + "<p>File Path: " + objectName + "</p>"
    	                + "<p>Thank you for your submission!</p>"
    	                + "<p>Best regards,<br/>The Assignment Team</p>";

    	        
    	        String fromUser = "demo6225@demo.csye6225hemangi.com";

             AmazonSimpleEmailService client =  AmazonSimpleEmailServiceClientBuilder.standard().build();
             SendEmailRequest request1 = new SendEmailRequest()
                          .withDestination(
                              new Destination().withToAddresses(username))
                          .withMessage(new com.amazonaws.services.simpleemail.model.Message()
                              .withBody(new Body()
                                  .withHtml(new Content()
                                      .withCharset("UTF-8").withData(bodyHtml))
                                  .withText(new Content()
                                      .withCharset("UTF-8").withData(bodyText)))
                              .withSubject(new Content()
                                  .withCharset("UTF-8").withData(subject)))
                          .withSource(fromUser);
                client.sendEmail(request1);
                System.out.println("Email sent!");
    	}
    	catch (Exception ex) {
    	      System.out.println("The email was not sent. Error message: " 
    	          + ex.getMessage());
    	    }


    }
	  
	private void sendEmailForInvalid(String assignmentName, String username, String submissionUrl, String submissionNumber) {
    	try {
    		 String subject = "Assignment Submission Status - " + assignmentName;
    		 String encodedUrl = URLEncoder.encode(submissionUrl, StandardCharsets.UTF_8.toString());

    		 String bodyText = "Dear " + username + ",\n\n"
    	                + "Unfortunately, your assignment " + assignmentName + " with attempt number " + submissionNumber + " has not been submitted successfully.\n\n"
    	                + "Please check the submission and try again.\n\n"
    	                + "Submission URL: " + encodedUrl + "\n"
    	                + "If you continue to experience issues, please contact our support team.\n\n"
    	                + "Best regards,\nThe Assignment Team";

    	        String bodyHtml = "<h2>Dear " + username + ",</h2>"
    	                + "<p>Unfortunately, your assignment " + assignmentName + " with attempt number " + submissionNumber + " has not been submitted successfully.</p>"
    	                + "<p>Please check the submission and try again.</p>"
    	                + "<p>Submission URL: <a href='" + encodedUrl + "'>" + encodedUrl + "</a></p>"
    	                + "<p>If you continue to experience issues, please contact our support team.</p>"
    	                + "<p>Best regards,<br/>The Assignment Team</p>";

             String fromUser = "demo6225@demo.csye6225hemangi.com";

             AmazonSimpleEmailService client =  AmazonSimpleEmailServiceClientBuilder.standard().build();
             SendEmailRequest request1 = new SendEmailRequest()
                          .withDestination(
                              new Destination().withToAddresses(username))
                          .withMessage(new com.amazonaws.services.simpleemail.model.Message()
                              .withBody(new Body()
                                  .withHtml(new Content()
                                      .withCharset("UTF-8").withData(bodyHtml))
                                  .withText(new Content()
                                      .withCharset("UTF-8").withData(bodyText)))
                              .withSubject(new Content()
                                  .withCharset("UTF-8").withData(subject)))
                          .withSource(fromUser);
                client.sendEmail(request1);
                System.out.println("Email sent!");
    	}
    	catch (Exception ex) {
    	      System.out.println("The email was not sent. Error message: " 
    	          + ex.getMessage());
    	    }


    }
	    
	   
	    
	  private InputStream downloadArtifact(String url) {
	        try {
	            URL artifactUrl = new URL(url);
	            HttpURLConnection connection = (HttpURLConnection) artifactUrl.openConnection();
	            connection.setRequestMethod("GET");
	            int responseCode = connection.getResponseCode();

	            if (responseCode == HttpURLConnection.HTTP_OK) {
	                urlDownloadFlag = true;
	                InputStream inputStream = connection.getInputStream();
	                System.out.println("Artifact downloaded " + url);
	                return inputStream;
	            } else {
	                // Set flag to false, indicating download failure
	                urlDownloadFlag = false;
	                System.out.println("Failed to download artifact, HTTP response code: " + responseCode);
	                return null;
	            }
	        } catch (Exception e) {
	            System.out.println("Exception during artifact download: " + e.getMessage());
	            // Set flag to false, indicating download failure
	            urlDownloadFlag = false;
	            return null;
	        }
	    }

	  
	private void storeInGoogleCloudStorage(String objectName, InputStream inputStream)
			throws Exception {

		String serviceAccountKey = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
		
		GoogleCredentials credentials;
		try (InputStream serviceAccountStream = new ByteArrayInputStream(serviceAccountKey.getBytes())) {
			credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		}

		Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		BlobId blobId = BlobId.of(GCS_BUCKET_NAME, objectName);

		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

		try (WriteChannel writer = storage.writer(blobInfo)) {
			byte[] buffer = new byte[1024];
			int limit;
			while ((limit = inputStream.read(buffer)) >= 0) {
				writer.write(ByteBuffer.wrap(buffer, 0, limit));
			}

		} catch (Exception ex) {
			System.out.println("Exception during upload: " + ex.getMessage());
		} finally {

			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					System.out.println("Error closing input stream: " + e.getMessage());
				}
			}
		}
		 System.out.println("Storage added : "+ blobInfo);
	}
	
	private void updateDynamoDB(String username, String assignmentName, String submissionId, String emailStatus) {
        try {
          
        	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
            // Put the item into the DynamoDB table
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", new AttributeValue(submissionId));
            item.put("username", new AttributeValue(username));
            item.put("assignmentName", new AttributeValue(assignmentName));
           //item.put("submissionNumber", new AttributeValue("1"));
            item.put("emailStatus", new AttributeValue(emailStatus));

            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName("assignment-email-table")
                    .withItem(item);

            client.putItem(putItemRequest);

            System.out.println("Added item: " + item);

           
        } catch (Exception e) {
        	 System.out.println("Exception during DynamoDB update: " + e.getMessage());
        }
    }

}
