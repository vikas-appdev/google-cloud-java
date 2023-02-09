package com.gradlic.google.dataflow;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.threeten.bp.Duration;

import java.io.FileInputStream;

@SpringBootApplication
public class DataflowApplication {

	public static void main(String[] args) throws Exception {
		//GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
		BigQuery bigQuery = BigQueryOptions.newBuilder()
				.setCredentials(GoogleCredentials.fromStream(new FileInputStream("C:\\Users\\vray9\\Downloads\\smooth-obelisk-377308-148459740cda.json")))
				.setProjectId("smooth-obelisk-377308")
				.build().getService();

		final String GET_WORD_COUNT = "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 1000";

		QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(GET_WORD_COUNT).build();

		Job queryJob = bigQuery.create(JobInfo.newBuilder(queryJobConfiguration).build());
		queryJob = queryJob.waitFor(RetryOption.initialRetryDelay(Duration.ZERO));

		if (queryJob == null){
			throw new Exception("Job no longer exists");
		}

		if (queryJob.getStatus().getError() !=null){
			throw new Exception(queryJob.getStatus().getError().toString());
		}

		System.out.println("word\tword_count");
		TableResult result = queryJob.getQueryResults();
		for (FieldValueList row : result.iterateAll()){
			String word = row.get("word").getStringValue();
			int wordCount = row.get("word_count").getNumericValue().intValue();
			System.out.printf("%s\t%d\n", word, wordCount);
		}

		//SpringApplication.run(DataflowApplication.class, args);

	}

}
