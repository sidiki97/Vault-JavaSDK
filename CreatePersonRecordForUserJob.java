package com.veeva.vault.custom.processors;


import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.PositionalRecordId;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.job.*;
import com.veeva.vault.sdk.api.query.Query;
import com.veeva.vault.sdk.api.query.QueryService;
import java.util.*;

@JobInfo(adminConfigurable = true)
public class CreatePersonRecordForUserJob implements Job {

    public JobInputSupplier init(JobInitContext jobInitContext) {
        return jobInitContext.newJobInput("SELECT id, first_name__sys, last_name__sys FROM user__sys");
    }

    public void process(JobProcessContext jobProcessContext) {
        List<JobItem> userIds = jobProcessContext.getCurrentTask().getItems();

        JobLogger logger = jobProcessContext.getJobLogger();

        // Map user id to bool (person record exists)
        @SuppressWarnings("unchecked")
        Map<String, Boolean> userPerson = VaultCollections.newMap();

        // Map user id to first and last name (required fields of person)
        @SuppressWarnings("unchecked")
        Map<String, List<String>> userInfo = VaultCollections.newMap();

        logger.log("Set maps userPerson and userInfo");
        for (JobItem user : userIds){
            String userId = user.getValue("id", JobValueType.STRING);
            userPerson.put(userId, false);
            String firstName = user.getValue("first_name__sys", JobValueType.STRING);
            String lastName = user.getValue("last_name__sys", JobValueType.STRING);

            @SuppressWarnings("unchecked")
            List<String> names = VaultCollections.newList();
            names.add(firstName);
            names.add(lastName);

            userInfo.put(userId, names);
        }


        QueryService queryService = ServiceLocator.locate(QueryService.class);


        Query query = queryService.newQueryBuilder().
                withSelect(VaultCollections.asList("vault_user__sysr.id")).
                withFrom("person__sys").
                withWhere("vault_user__sysr.id != null").
                build();


        RecordService recordService = ServiceLocator.locate(RecordService.class);

        logger.log("Run query "  + query.toString());
        queryService.query(
                queryService.newQueryExecutionRequestBuilder().withQuery(query).build()
        ).onSuccess(
                queryExecutionResponse -> {
                    queryExecutionResponse.streamResults().forEach(
                    queryExecutionResult -> {
                        String queryReturn = queryExecutionResult.getValue("vault_user__sysr.id", ValueType.STRING);
                        // Set true in userPerson map for user id with person record
                        userPerson.replace(queryReturn, true);
                    }
                    );
                }
        ).onError(
                queryOperationError -> {
                    queryOperationError.getQueryOperationErrorType();
                }
        ).execute();

        @SuppressWarnings("unchecked")
        List<Record> newRecords = VaultCollections.newList();

        // Create, set values, and add new person records to list
        for(Map.Entry<String, Boolean> single : userPerson.entrySet()){
            if (!single.getValue()){
                logger.log("Set values on new record");
                Record newPerson = recordService.newRecord("person__sys");
                newPerson.setValue("first_name__sys", userInfo.get(single.getKey()).get(0));
                newPerson.setValue("last_name__sys", userInfo.get(single.getKey()).get(1));
                newPerson.setValue("vault_user__sys", single.getKey());

                logger.log("Add new person record to list");
                newRecords.add(newPerson);
            }
        }

        JobTask task = jobProcessContext.getCurrentTask();
        TaskOutput taskOutput = task.getTaskOutput();

        BatchOperation<PositionalRecordId, BatchOperationError> batchSaveResult = recordService.batchSaveRecords(newRecords);

        batchSaveResult.onSuccesses(positionalRecordIds -> {
            taskOutput.setState(TaskState.SUCCESS);
            logger.log("Person Record Save successful");
        });

        batchSaveResult.onErrors( batchOperationErrors -> {
            taskOutput.setState(TaskState.ERRORS_ENCOUNTERED);
            taskOutput.setValue("firstError", batchOperationErrors.get(0).getError().getMessage());
            logger.log("Person Record Save unsuccessful due to: " + batchOperationErrors.get(0).getError().getMessage());
        });

        batchSaveResult.execute();

    }

    public void completeWithSuccess(JobCompletionContext jobCompletionContext) {
        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("All tasks completed successfully");
    }

    public void completeWithError(JobCompletionContext jobCompletionContext) {
        JobResult result = jobCompletionContext.getJobResult();

        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("completeWithError: " + result.getNumberFailedTasks() + "tasks failed out of " + result.getNumberTasks());

        List<JobTask> tasks = jobCompletionContext.getTasks();
        for (JobTask task : tasks) {
            TaskOutput taskOutput = task.getTaskOutput();
            if (TaskState.ERRORS_ENCOUNTERED.equals(taskOutput.getState())) {
                logger.log(task.getTaskId() + " failed with error message " + taskOutput.getValue("firstError", JobValueType.STRING));
            }
        }
    }


}