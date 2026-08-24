#include <Server/StatelessWorker/StatelessWorkerEndpoint.h>
#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Core/ProtocolDefines.h>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

StatelessWorkerEndpoint::StatelessWorkerEndpoint(size_t max_threads, size_t max_free_threads, size_t queue_size)
    : endpoint_name("stateless_worker/")
    , log(Poco::Logger::getShared("StatelessWorkerEndpoint"))
    , task_runner(std::make_shared<StatelessTaskExecutor>(max_threads, max_free_threads, queue_size))
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
    writeVarUInt(task_description.serialization_version, out);

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
    for (const auto & [stream, address] : task_description.exchange_stream_sources.stream_hosts)
    {
        writeStringBinary(stream, out);
        writeStringBinary(address.host, out);
        if (task_description.serialization_version >= 2)
            writeVarUInt(address.port, out);
    }

    writeVarUInt(task_description.settings_changes.size(), out);
    for (const auto & change : task_description.settings_changes)
    {
        writeStringBinary(change.name, out);
        writeFieldBinary(change.value, out);
    }

    if (task_description.serialization_version < 3 && !task.runtime_filter_descriptors.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Distributed task serialization version {} cannot carry runtime filter receive descriptors",
            task_description.serialization_version);

    if (task_description.serialization_version >= 3)
    {
        writeVarUInt(task.runtime_filter_descriptors.size(), out);
        for (const auto & descriptor : task.runtime_filter_descriptors)
        {
            writeStringBinary(descriptor.filter_key, out);
            writeStringBinary(descriptor.filter_name, out);
            encodeDataType(descriptor.key_column_type, out);
            writeVarUInt(descriptor.geometry.exact_values_limit, out);
            writeVarUInt(descriptor.geometry.exact_bytes_limit, out);
            writeVarUInt(descriptor.geometry.bloom_filter_bytes, out);
            writeVarUInt(descriptor.geometry.bloom_filter_hash_functions, out);
            writeBinary(descriptor.geometry.pass_ratio_threshold_for_disabling, out);
            writeVarUInt(descriptor.geometry.blocks_to_skip_before_reenabling, out);
            writeBinary(descriptor.geometry.max_ratio_of_set_bits_in_bloom_filter, out);
            writeVarUInt(descriptor.streams.size(), out);
            for (const auto & stream : descriptor.streams)
            {
                writeStringBinary(stream.exchange_id, out);
                writeStringBinary(stream.source_bucket, out);
                writeStringBinary(stream.destination_bucket, out);
            }
        }
    }
}

void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in)
{
    UInt64 version = 0;
    readVarUInt(version, in);
    if (version > DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Distributed task serialization version {} is not supported. The last supported version is {}",
            version, DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);

    readStringBinary(task_description.initial_query_id, in);

    auto & task = task_description.task;

    readStringBinary(task.task_id, in);
    readStringBinary(task_description.serialized_query_plan, in);

    size_t parameters_size = 0;
    readVarUInt(parameters_size, in);
    for (size_t i = 0; i < parameters_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        Field field = readFieldBinary(in);
        task.parameters.parameters[name] = field;
    }

    size_t input_files_size = 0;
    readVarUInt(input_files_size, in);
    task.input_exchange_streams.resize(input_files_size);
    for (size_t i = 0; i < input_files_size; ++i)
    {
        readStringBinary(task.input_exchange_streams[i].exchange_id, in);
        readStringBinary(task.input_exchange_streams[i].source_bucket, in);
        readStringBinary(task.input_exchange_streams[i].destination_bucket, in);
    }

    size_t output_files_size = 0;
    readVarUInt(output_files_size, in);
    task.output_exchange_streams.resize(output_files_size);
    for (size_t i = 0; i < output_files_size; ++i)
    {
        readStringBinary(task.output_exchange_streams[i].exchange_id, in);
        readStringBinary(task.output_exchange_streams[i].source_bucket, in);
        readStringBinary(task.output_exchange_streams[i].destination_bucket, in);
    }

    size_t exchanges_size = 0;
    readVarUInt(exchanges_size, in);
    for (size_t i = 0; i < exchanges_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        ExchangeDescription exchange;
        UInt64 kind = 0;
        readVarUInt(kind, in);
        exchange.kind = static_cast<ExchangeDescription::Kind>(kind);
        readVarUInt(exchange.source_bucket_count, in);
        readVarUInt(exchange.destination_bucket_count, in);
        task_description.exchanges[name] = exchange;
    }

    size_t exchange_stream_sources_size = 0;
    readVarUInt(exchange_stream_sources_size, in);
    for (size_t i = 0; i < exchange_stream_sources_size; ++i)
    {
        String stream;
        readStringBinary(stream, in);
        StreamSourceAddress address;
        readStringBinary(address.host, in);
        if (version >= 2)
        {
            UInt64 port = 0;
            readVarUInt(port, in);
            address.port = static_cast<UInt16>(port);
        }
        task_description.exchange_stream_sources.stream_hosts[stream] = address;
    }

    if (version >= 1)
    {
        size_t settings_changes_size = 0;
        readVarUInt(settings_changes_size, in);
        task_description.settings_changes.reserve(settings_changes_size);
        for (size_t i = 0; i < settings_changes_size; ++i)
        {
            String name;
            readStringBinary(name, in);
            Field value = readFieldBinary(in);
            task_description.settings_changes.emplace_back(name, value);
        }
    }

    if (version >= 3)
    {
        size_t descriptors_size = 0;
        readVarUInt(descriptors_size, in);
        /// A legitimate initiator emits one descriptor per admitted filter and one stream per
        /// build/root task.
        constexpr size_t max_runtime_filter_receive_descriptors = 1000;
        constexpr size_t max_streams_per_runtime_filter_descriptor = 1000;
        if (descriptors_size > max_runtime_filter_receive_descriptors)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Too many runtime filter receive descriptors: {}", descriptors_size);
        task.runtime_filter_descriptors.resize(descriptors_size);
        std::unordered_set<String> seen_filter_keys;
        for (size_t i = 0; i < descriptors_size; ++i)
        {
            auto & descriptor = task.runtime_filter_descriptors[i];
            readStringBinary(descriptor.filter_key, in);
            readStringBinary(descriptor.filter_name, in);
            /// Trusted server-to-server task: decode types without the input complexity limit.
            descriptor.key_column_type = decodeDataType(in, /*max_complexity=*/0);
            readVarUInt(descriptor.geometry.exact_values_limit, in);
            readVarUInt(descriptor.geometry.exact_bytes_limit, in);
            readVarUInt(descriptor.geometry.bloom_filter_bytes, in);
            readVarUInt(descriptor.geometry.bloom_filter_hash_functions, in);
            readBinary(descriptor.geometry.pass_ratio_threshold_for_disabling, in);
            readVarUInt(descriptor.geometry.blocks_to_skip_before_reenabling, in);
            readBinary(descriptor.geometry.max_ratio_of_set_bits_in_bloom_filter, in);
            size_t streams_size = 0;
            readVarUInt(streams_size, in);
            if (streams_size > max_streams_per_runtime_filter_descriptor)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too many streams on a runtime filter receive descriptor: {}", streams_size);
            descriptor.streams.resize(streams_size);
            for (size_t j = 0; j < streams_size; ++j)
            {
                readStringBinary(descriptor.streams[j].exchange_id, in);
                readStringBinary(descriptor.streams[j].source_bucket, in);
                readStringBinary(descriptor.streams[j].destination_bucket, in);
            }

            if (descriptor.filter_key.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Runtime filter receive descriptor has an empty filter key");
            if (descriptor.streams.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Runtime filter receive descriptor has no streams");
            if (!seen_filter_keys.insert(descriptor.filter_key).second)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Duplicate runtime filter receive descriptor for key {}", descriptor.filter_key);
            descriptor.geometry.validateTransported();
        }
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
                /// A gone task is a normal status answer, not a transport error: it has already
                /// finished and been reclaimed, or was never started. Report it as a successful status
                /// query carrying an "Unknown task" state so the coordinator treats it as terminal.
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
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

void StatelessWorkerEndpoint::shutdown()
{
    task_runner->shutdown();
}

}
