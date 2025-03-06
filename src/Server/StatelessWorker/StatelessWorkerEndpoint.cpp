#include <Server/StatelessWorker/StatelessWorkerEndpoint.h>
#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include "Processors/QueryPlan/QueryPlan.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StatelessWorkerEndpoint::StatelessWorkerEndpoint()
    : endpoint_name("stateless_worker/")
    , log(Poco::Logger::getShared("StatelessWorkerEndpoint"))
    , task_runner(std::make_shared<StatelessTaskExecutor>())
{
}

StatelessWorkerEndpoint::~StatelessWorkerEndpoint()
{
    shutdown();
}

std::string StatelessWorkerEndpoint::getId(const std::string & path) const
{
    return endpoint_name + path;
}

void serializeTask(const DistributedQueryTask & task, const String & serialized_query_plan, WriteBuffer & out)
{
    writeStringBinary(task.task_id, out);
    writeStringBinary(serialized_query_plan, out);

    writeVarUInt(task.parameters.parameters.size(), out);
    for (const auto & [name, field] : task.parameters.parameters)
    {
        writeStringBinary(name, out);
        writeFieldBinary(field, out);
    }

    writeVarUInt(task.input_temporary_files.size(), out);
    for (const auto & file : task.input_temporary_files)
        writeStringBinary(file, out);

    writeVarUInt(task.output_temporary_files.size(), out);
    for (const auto & file : task.output_temporary_files)
        writeStringBinary(file, out);
}

void deserializeTask(DistributedQueryTask & task, String & serialized_query_plan, ReadBuffer & in)
{
    readStringBinary(task.task_id, in);
    readStringBinary(serialized_query_plan, in);

    size_t parameters_size;
    readVarUInt(parameters_size, in);
    for (size_t i = 0; i < parameters_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        Field field = readFieldBinary(in);
        task.parameters.parameters[name] = field;
    }

    size_t input_files_size;
    readVarUInt(input_files_size, in);
    task.input_temporary_files.resize(input_files_size);
    for (size_t i = 0; i < input_files_size; ++i)
        readStringBinary(task.input_temporary_files[i], in);

    size_t output_files_size;
    readVarUInt(output_files_size, in);
    task.output_temporary_files.resize(output_files_size);
    for (size_t i = 0; i < output_files_size; ++i)
        readStringBinary(task.output_temporary_files[i], in);
}

void StatelessWorkerEndpoint::processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response)
{
    auto operation = params.get("operation");
    auto task_id = params.get("task_id");

    if (operation == "start")
    {
        auto unique_temp_file_path = params.get("temp_path");
        /// Deserialize task fields from the request body
        DistributedQueryTask task_parameters;
        String serialized_query_plan;
        deserializeTask(task_parameters, serialized_query_plan, body);
        body.eof();

        /// Pass it to the runner to start execution
        task_runner->startTask(task_id, serialized_query_plan, task_parameters, unique_temp_file_path);
    }
    else if (operation == "get_status")
    {
        UInt64 wait_milliseconds = 0;
        if (params.has("wait_for_ms"))
        {
            wait_milliseconds = parse<UInt64>(params.get("wait_for_ms"));
        }

        body.eof();
        auto status = task_runner->getStatus(task_id, wait_milliseconds);
        switch (status.result)
        {
            case StatelessTaskExecutor::TaskRunnig:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Running\n", out);
                break;
            }
            case StatelessTaskExecutor::TaskFinished:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Finished\n", out);
                break;
            }
            case StatelessTaskExecutor::TaskCancelled:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Cancelled\n", out);
                break;
            }
            case StatelessTaskExecutor::UnknownTaskId:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                writeString("Unknown task\n", out);
                break;
            }
            default:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                writeString(fmt::format("Unexpected task status: {}\n", static_cast<int>(status.result)), out);
                break;
            }
        }
    }
    else if (operation == "cancel")
    {
        body.eof();
        auto result = task_runner->cancelTask(task_id);
        switch (result)
        {
            case StatelessTaskExecutor::TaskCancelled:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                writeString("Cancelled\n", out);
                break;
            }
            case StatelessTaskExecutor::UnknownTaskId:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
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
        body.eof();
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
        body.eof();
        LOG_WARNING(log, "Unsupported operation '{}'", operation);
        response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        writeString("Unknown operation type\n", out);
    }
}

void StatelessWorkerEndpoint::shutdown()
{
    task_runner->shutdown();
}

}
