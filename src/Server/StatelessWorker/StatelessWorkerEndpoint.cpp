#include <Server/StatelessWorker/StatelessWorkerEndpoint.h>
#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out)
{
    writeVarUInt(DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION, out);

    writeStringBinary(task_description.initial_query_id, out);

    const auto & task = task_description.task;

    writeStringBinary(task.task_id, out);
    writeStringBinary(task_description.serialized_query_plan, out);

    writeVarUInt(task.parameters.parameters.size(), out);
    for (const auto & [name, field] : task.parameters.parameters)
    {
        writeStringBinary(name, out);
        writeFieldBinary(field, out);
    }

    writeVarUInt(task.input_exchange_streams.size(), out);
    for (const auto & stream_id : task.input_exchange_streams)
    {
        writeStringBinary(stream_id.exchange_id, out);
        writeStringBinary(stream_id.source_bucket, out);
        writeStringBinary(stream_id.destination_bucket, out);
    }

    writeVarUInt(task.output_exchange_streams.size(), out);
    for (const auto & stream_id : task.output_exchange_streams)
    {
        writeStringBinary(stream_id.exchange_id, out);
        writeStringBinary(stream_id.source_bucket, out);
        writeStringBinary(stream_id.destination_bucket, out);
    }

    writeVarUInt(task_description.exchanges.size(), out);
    for (const auto & [name, exchange] : task_description.exchanges)
    {
        chassert(name == exchange.name);
        writeStringBinary(exchange.name, out);
        writeVarUInt(static_cast<size_t>(exchange.kind), out);
        writeVarUInt(exchange.source_bucket_count, out);
        writeVarUInt(exchange.destination_bucket_count, out);
    }

    writeVarUInt(task_description.exchange_stream_sources.stream_hosts.size(), out);
    for (const auto & [stream, host] : task_description.exchange_stream_sources.stream_hosts)
    {
        writeStringBinary(stream, out);
        writeStringBinary(host, out);
    }
}

void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in)
{
    UInt64 version;
    readVarUInt(version, in);
    if (version > DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Distributed task serialization version {} is not supported. The last supported version is {}",
            version, DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);

    readStringBinary(task_description.initial_query_id, in);

    auto & task = task_description.task;

    readStringBinary(task.task_id, in);
    readStringBinary(task_description.serialized_query_plan, in);

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
    task.input_exchange_streams.resize(input_files_size);
    for (size_t i = 0; i < input_files_size; ++i)
    {
        readStringBinary(task.input_exchange_streams[i].exchange_id, in);
        readStringBinary(task.input_exchange_streams[i].source_bucket, in);
        readStringBinary(task.input_exchange_streams[i].destination_bucket, in);
    }

    size_t output_files_size;
    readVarUInt(output_files_size, in);
    task.output_exchange_streams.resize(output_files_size);
    for (size_t i = 0; i < output_files_size; ++i)
    {
        readStringBinary(task.output_exchange_streams[i].exchange_id, in);
        readStringBinary(task.output_exchange_streams[i].source_bucket, in);
        readStringBinary(task.output_exchange_streams[i].destination_bucket, in);
    }

    size_t exchanges_size;
    readVarUInt(exchanges_size, in);
    for (size_t i = 0; i < exchanges_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        ExchangeDescription exchange;
        UInt64 kind;
        readVarUInt(kind, in);
        exchange.kind = static_cast<ExchangeDescription::Kind>(kind);
        readVarUInt(exchange.source_bucket_count, in);
        readVarUInt(exchange.destination_bucket_count, in);
        task_description.exchanges[name] = exchange;
    }

    size_t exchange_stream_sources_size;
    readVarUInt(exchange_stream_sources_size, in);
    for (size_t i = 0; i < exchange_stream_sources_size; ++i)
    {
        String stream;
        readStringBinary(stream, in);
        String host;
        readStringBinary(host, in);
        task_description.exchange_stream_sources.stream_hosts[stream] = host;
    }
}

void StatelessWorkerEndpoint::processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response)
{
    auto operation = params.get("operation");
    auto task_id = params.get("task_id");

    if (operation == "start")
    {
        auto unique_temp_file_path = params.get("temp_path");
        /// Deserialize task fields from the request body
        DistributedQueryTaskDescription task_description;
        deserializeTask(task_description, *body);
        body->eof();
        body.reset();

        /// Pass it to the runner to start execution
        task_runner->startTask(task_id, task_description, unique_temp_file_path);
    }
    else if (operation == "get_status")
    {
        UInt64 wait_milliseconds = 0;
        if (params.has("wait_for_ms"))
            wait_milliseconds = parse<UInt64>(params.get("wait_for_ms"));

        UInt64 client_version = DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS;
        if (params.has("client_version"))
            client_version = parse<UInt64>(params.get("client_version"));

        body->eof();
        body.reset();

        auto status = task_runner->getStatus(task_id, wait_milliseconds);
        DistributedQueryTaskStatus task_status;
        task_status.progress = std::move(status.progress);

        switch (status.result)
        {
            case StatelessTaskExecutor::TaskRunnig:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Running";
                break;
            }
            case StatelessTaskExecutor::TaskFinished:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Finished";
                break;
            }
            case StatelessTaskExecutor::TaskCancelled:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Cancelled";
                break;
            }
            case StatelessTaskExecutor::TaskFailed:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Failed";
                task_status.error_message = status.message;
                break;
            }
            case StatelessTaskExecutor::UnknownTaskId:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                task_status.status = "Unknown task";
                break;
            }
            default:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                task_status.status = fmt::format("Unexpected task status: {}", static_cast<int>(status.result));
                break;
            }
        }
        task_status.write(out, client_version);
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

void StatelessWorkerEndpoint::shutdown()
{
    task_runner->shutdown();
}

}
