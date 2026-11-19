#include <Server/StatelessWorker/StatelessWorkerEndpoint.h>
#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

namespace FailPoints
{
    extern const char stateless_worker_pause_before_start_task[];
}

StatelessWorkerEndpoint::StatelessWorkerEndpoint(std::shared_ptr<StatelessTaskExecutor> task_runner_)
    : endpoint_name("stateless_worker/")
    , log(Poco::Logger::getShared("StatelessWorkerEndpoint"))
    , task_runner(std::move(task_runner_))
{
}

std::string StatelessWorkerEndpoint::getId(const std::string & path) const
{
    return endpoint_name + path;
}

void StatelessWorkerEndpoint::processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response)
{
    auto operation = params.get("operation");
    if (operation == "get_statuses" || operation == "cancel_tasks" || operation == "forget_tasks")
    {
        Strings task_ids = readTaskIds(*body);
        /// Read the rest of the body, otherwise the keep-alive connection is closed.
        body->ignoreAll();
        body.reset();

        if (operation == "cancel_tasks")
        {
            task_runner->cancelTasks(task_ids);
            return;
        }
        if (operation == "forget_tasks")
        {
            task_runner->forgetTasks(task_ids);
            return;
        }

        UInt64 wait_milliseconds = 0;
        if (params.has("wait_for_ms"))
            wait_milliseconds = parse<UInt64>(params.get("wait_for_ms"));
        writeTaskStatuses(makeTaskStatusEntries(task_ids, task_runner->getStatuses(task_ids, wait_milliseconds)), out);
        return;
    }

    if (operation == "start_tasks")
    {
        auto unique_query_id = params.get("query_id");
        auto unique_temp_file_path = params.get("temp_path");
        TaskCollectors collectors;
        if (params.has("collect"))
            collectors = TaskCollectors::parse(params.get("collect"));

        DistributedQueryTaskBatch batch;
        deserializeTaskBatch(batch, *body);
        /// Read the rest of the body, otherwise the keep-alive connection is closed.
        body->ignoreAll();
        body.reset();

        /// Holds a start that is on the wire, so a cancel can overtake it.
        FailPointInjection::pauseFailPoint(FailPoints::stateless_worker_pause_before_start_task);

        if (!task_runner->startTasks(unique_query_id, std::move(batch), unique_temp_file_path, collectors))
        {
            /// The coordinator reads 429 as a busy server (`RECEIVED_ERROR_TOO_MANY_REQUESTS`).
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS);
            writeString("The stateless worker runs `stateless_worker_server.max_queries` queries already", out);
        }
        return;
    }

    auto task_id = params.get("task_id");

    if (operation == "start")
    {
        auto unique_temp_file_path = params.get("temp_path");
        /// What the coordinator wants appended to status replies; absent means the fixed reply only.
        TaskCollectors collectors;
        if (params.has("collect"))
            collectors = TaskCollectors::parse(params.get("collect"));

        /// Deserialize task fields from the request body
        /// This could have been moved to a thread, but it would still require a copy of an object
        /// and for now it is not expensive part
        DistributedQueryTaskDescription task_description;
        deserializeTask(task_description, *body);
        /// Read the rest of the body, otherwise the keep-alive connection is closed.
        body->ignoreAll();
        body.reset();

        /// Holds a start that is on the wire, so a cancel can overtake it.
        FailPointInjection::pauseFailPoint(FailPoints::stateless_worker_pause_before_start_task);

        /// Pass it to the runner to start execution
        task_runner->startTask(task_id, std::move(task_description), unique_temp_file_path, collectors);
    }
    else if (operation == "get_status")
    {
        UInt64 wait_milliseconds = 0;
        if (params.has("wait_for_ms"))
            wait_milliseconds = parse<UInt64>(params.get("wait_for_ms"));

        body->eof();
        body.reset();

        auto status = task_runner->getStatus(task_id, wait_milliseconds);
        const TaskCollectors collectors = status.collectors;
        DistributedQueryTaskStatus task_status;
        response.setStatus(makeTaskStatusReply(std::move(status), task_status)
            ? Poco::Net::HTTPResponse::HTTP_OK : Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        task_status.write(out, collectors);
    }
    else if (operation == "cancel")
    {
        body->eof();
        body.reset();
        auto result = task_runner->cancelTask(task_id);
        switch (result)
        {
            case StatelessTaskExecutor::Ok:
            case StatelessTaskExecutor::TaskCancelled:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Cancelled\n", out);
                break;
            }
            case StatelessTaskExecutor::UnknownTaskId:
            {
                /// For idempotency: a gone task is already not running, so cancellation succeeded.
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Unknown task\n", out);
                break;
            }
            default:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                writeString(fmt::format("Unexpected task status: {}\n", static_cast<int>(result)), out);
                break;
            }
        }
    }
    else if (operation == "forget")
    {
        body->eof();
        body.reset();
        auto result = task_runner->forgetTask(task_id);
        switch (result)
        {
            case StatelessTaskExecutor::Ok:
            case StatelessTaskExecutor::UnknownTaskId: /// For idempotency
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                break;
            }
            default:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                writeString(fmt::format("Unexpected task status: {}\n", static_cast<int>(result)), out);
                break;
            }
        }
    }
    else
    {
        body->eof();
        body.reset();
        LOG_WARNING(log, "Unsupported operation '{}'", operation);
        response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        writeString("Unknown operation type\n", out);
    }
}

}
