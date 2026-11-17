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
#include <Common/FailPoint.h>
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

namespace FailPoints
{
    extern const char stateless_worker_pause_before_start_task[];
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

namespace
{

void writeParameters(const QueryPlanParameters & parameters, WriteBuffer & out)
{
    writeVarUInt(parameters.parameters.size(), out);
    for (const auto & [name, field] : parameters.parameters)
    {
        writeStringBinary(name, out);
        writeFieldBinary(field, out);
    }
}

void readParameters(QueryPlanParameters & parameters, ReadBuffer & in)
{
    size_t parameters_size = 0;
    readVarUInt(parameters_size, in);
    for (size_t i = 0; i < parameters_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        Field field = readFieldBinary(in);
        parameters.parameters[name] = field;
    }
}

void writeExchangeStreams(const VectorWithMemoryTracking<ExchangeStreamId> & streams, WriteBuffer & out)
{
    writeVarUInt(streams.size(), out);
    for (const auto & stream_id : streams)
    {
        writeStringBinary(stream_id.exchange_id, out);
        writeStringBinary(stream_id.source_bucket, out);
        writeStringBinary(stream_id.destination_bucket, out);
    }
}

void readExchangeStreams(VectorWithMemoryTracking<ExchangeStreamId> & streams, ReadBuffer & in)
{
    size_t streams_size = 0;
    readVarUInt(streams_size, in);
    streams.resize(streams_size);
    for (auto & stream_id : streams)
    {
        readStringBinary(stream_id.exchange_id, in);
        readStringBinary(stream_id.source_bucket, in);
        readStringBinary(stream_id.destination_bucket, in);
    }
}

void writeExchanges(const ExchangeDescriptions & exchanges, WriteBuffer & out)
{
    writeVarUInt(exchanges.size(), out);
    for (const auto & [name, exchange] : exchanges)
    {
        chassert(name == exchange.name);
        writeStringBinary(exchange.name, out);
        writeVarUInt(static_cast<size_t>(exchange.kind), out);
        writeVarUInt(exchange.source_bucket_count, out);
        writeVarUInt(exchange.destination_bucket_count, out);
    }
}

void readExchanges(ExchangeDescriptions & exchanges, ReadBuffer & in)
{
    size_t exchanges_size = 0;
    readVarUInt(exchanges_size, in);
    for (size_t i = 0; i < exchanges_size; ++i)
    {
        ExchangeDescription exchange;
        readStringBinary(exchange.name, in);
        UInt64 kind = 0;
        readVarUInt(kind, in);
        if (kind != static_cast<UInt64>(ExchangeDescription::Kind::Persisted)
            && kind != static_cast<UInt64>(ExchangeDescription::Kind::Streaming))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown exchange kind {}", kind);
        exchange.kind = static_cast<ExchangeDescription::Kind>(kind);
        readVarUInt(exchange.source_bucket_count, in);
        readVarUInt(exchange.destination_bucket_count, in);
        exchanges[exchange.name] = exchange;
    }
}

void writeStreamSources(const ExchangeStreamSources & stream_sources, UInt64 version, WriteBuffer & out)
{
    writeVarUInt(stream_sources.stream_hosts.size(), out);
    for (const auto & [stream, address] : stream_sources.stream_hosts)
    {
        writeStringBinary(stream, out);
        writeStringBinary(address.host, out);
        if (version >= 2)
            writeVarUInt(address.port, out);
    }
}

void readStreamSources(ExchangeStreamSources & stream_sources, UInt64 version, ReadBuffer & in)
{
    size_t exchange_stream_sources_size = 0;
    readVarUInt(exchange_stream_sources_size, in);
    for (size_t i = 0; i < exchange_stream_sources_size; ++i)
    {
        String stream;
        readStringBinary(stream, in);
        StreamSourceAddress address;
        readStringBinary(address.host, in);
        if (version >= 2)
            readVarUInt(address.port, in);
        stream_sources.stream_hosts[stream] = address;
    }
}

void writeSettingsChanges(const SettingsChanges & settings_changes, WriteBuffer & out)
{
    writeVarUInt(settings_changes.size(), out);
    for (const auto & change : settings_changes)
    {
        writeStringBinary(change.name, out);
        writeFieldBinary(change.value, out);
    }
}

void readSettingsChanges(SettingsChanges & settings_changes, ReadBuffer & in)
{
    size_t settings_changes_size = 0;
    readVarUInt(settings_changes_size, in);
    settings_changes.reserve(settings_changes_size);
    for (size_t i = 0; i < settings_changes_size; ++i)
    {
        String name;
        readStringBinary(name, in);
        Field value = readFieldBinary(in);
        settings_changes.emplace_back(name, value);
    }
}

void writeRuntimeFilterDescriptors(const VectorWithMemoryTracking<RuntimeFilterReceiveDescriptor> & descriptors, UInt64 version, WriteBuffer & out)
{
    if (version < DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_RUNTIME_FILTERS)
    {
        if (!descriptors.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Distributed task serialization version {} cannot carry runtime filter receive descriptors",
                version);
        return;
    }

    writeVarUInt(descriptors.size(), out);
    for (const auto & descriptor : descriptors)
    {
        writeStringBinary(descriptor.filter_key, out);
        writeStringBinary(descriptor.filter_name, out);
        encodeDataType(descriptor.key_column_type, out);
        descriptor.geometry.write(out);
        writeStringBinary(descriptor.stream.exchange_id, out);
        writeStringBinary(descriptor.stream.source_bucket, out);
        writeStringBinary(descriptor.stream.destination_bucket, out);
    }
}

void readRuntimeFilterDescriptors(VectorWithMemoryTracking<RuntimeFilterReceiveDescriptor> & descriptors, UInt64 version, ReadBuffer & in)
{
    if (version < DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_RUNTIME_FILTERS)
        return;

    size_t descriptors_size = 0;
    readVarUInt(descriptors_size, in);
    /// Limits against a corrupt task; real tasks are far below them.
    constexpr size_t max_runtime_filter_receive_descriptors = 1000;
    if (descriptors_size > max_runtime_filter_receive_descriptors)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Too many runtime filter receive descriptors: {}", descriptors_size);
    descriptors.resize(descriptors_size);
    std::unordered_set<String> seen_filter_keys;
    for (size_t i = 0; i < descriptors_size; ++i)
    {
        auto & descriptor = descriptors[i];
        readStringBinary(descriptor.filter_key, in);
        readStringBinary(descriptor.filter_name, in);
        /// Trusted server-to-server task: decode types without the input complexity limit.
        descriptor.key_column_type = decodeDataType(in, /*max_complexity=*/0);
        descriptor.geometry = RuntimeFilterGeometry::read(in);
        readStringBinary(descriptor.stream.exchange_id, in);
        readStringBinary(descriptor.stream.source_bucket, in);
        readStringBinary(descriptor.stream.destination_bucket, in);

        if (descriptor.filter_key.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Runtime filter receive descriptor has an empty filter key");
        if (descriptor.stream.exchange_id.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Runtime filter receive descriptor has no exchange");
        if (!seen_filter_keys.insert(descriptor.filter_key).second)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Duplicate runtime filter receive descriptor for key {}", descriptor.filter_key);
        descriptor.geometry.validateTransported();
    }
}

}

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out)
{
    serializeTask(task_description, task_description.task, task_description.exchange_stream_sources, out);
}

void serializeTask(const DistributedQueryTaskDescription & task_description, const DistributedQueryTask & task,
    const ExchangeStreamSources & stream_sources, WriteBuffer & out)
{
    const UInt64 version = task_description.serialization_version;
    writeVarUInt(version, out);
    writeStringBinary(task_description.initial_query_id, out);
    writeStringBinary(task.task_id, out);
    chassert(task_description.serialized_query_plan);
    writeStringBinary(*task_description.serialized_query_plan, out);
    writeParameters(task.parameters, out);
    writeExchangeStreams(task.input_exchange_streams, out);
    writeExchangeStreams(task.output_exchange_streams, out);
    writeExchanges(task_description.exchanges, out);
    writeStreamSources(stream_sources, version, out);
    writeSettingsChanges(task_description.settings_changes, out);
    writeRuntimeFilterDescriptors(task.runtime_filter_descriptors, version, out);
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
    String serialized_query_plan;
    readStringBinary(serialized_query_plan, in);
    task_description.serialized_query_plan = std::make_shared<const String>(std::move(serialized_query_plan));
    readParameters(task.parameters, in);
    readExchangeStreams(task.input_exchange_streams, in);
    readExchangeStreams(task.output_exchange_streams, in);
    readExchanges(task_description.exchanges, in);
    readStreamSources(task_description.exchange_stream_sources, version, in);
    if (version >= 1)
        readSettingsChanges(task_description.settings_changes, in);
    readRuntimeFilterDescriptors(task.runtime_filter_descriptors, version, in);
}

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out)
{
    const UInt64 version = batch.serialization_version;
    if (version < DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_TASK_BATCHES)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed task serialization version {} cannot carry a task batch", version);

    writeVarUInt(version, out);
    writeStringBinary(batch.initial_query_id, out);
    writeExchanges(batch.exchanges, out);
    writeSettingsChanges(batch.settings_changes, out);
    writeVarUInt(batch.stages.size(), out);
    for (const auto & stage : batch.stages)
    {
        writeStringBinary(stage.name, out);
        chassert(stage.serialized_query_plan);
        writeStringBinary(*stage.serialized_query_plan, out);
        writeVarUInt(stage.tasks.size(), out);
        for (const auto & [task, stream_sources] : stage.tasks)
        {
            writeStringBinary(task.task_id, out);
            writeParameters(task.parameters, out);
            writeExchangeStreams(task.input_exchange_streams, out);
            writeExchangeStreams(task.output_exchange_streams, out);
            writeStreamSources(stream_sources, version, out);
            writeRuntimeFilterDescriptors(task.runtime_filter_descriptors, version, out);
        }
    }
}

void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in)
{
    UInt64 version = 0;
    readVarUInt(version, in);
    if (version > DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Distributed task serialization version {} is not supported. The last supported version is {}",
            version, DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);
    if (version < DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_TASK_BATCHES)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Distributed task serialization version {} cannot carry a task batch", version);
    batch.serialization_version = version;

    readStringBinary(batch.initial_query_id, in);
    readExchanges(batch.exchanges, in);
    readSettingsChanges(batch.settings_changes, in);

    /// Limits against a corrupt batch; real ones are far below them.
    constexpr size_t max_stages = 10000;
    constexpr size_t max_tasks = 100000;
    size_t stages_size = 0;
    readVarUInt(stages_size, in);
    if (stages_size == 0 || stages_size > max_stages)
        throw Exception(ErrorCodes::INCORRECT_DATA, "A task batch must have between 1 and {} stages, got {}", max_stages, stages_size);
    batch.stages.resize(stages_size);

    std::unordered_set<String> seen_stages;
    std::unordered_set<String> seen_tasks;
    for (auto & stage : batch.stages)
    {
        readStringBinary(stage.name, in);
        if (!seen_stages.insert(stage.name).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate stage {} in a task batch", stage.name);
        String serialized_query_plan;
        readStringBinary(serialized_query_plan, in);
        stage.serialized_query_plan = std::make_shared<const String>(std::move(serialized_query_plan));

        size_t tasks_size = 0;
        readVarUInt(tasks_size, in);
        if (tasks_size == 0 || seen_tasks.size() + tasks_size > max_tasks)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Stage {} of a task batch has {} tasks", stage.name, tasks_size);
        stage.tasks.resize(tasks_size);
        for (auto & [task, stream_sources] : stage.tasks)
        {
            readStringBinary(task.task_id, in);
            if (!seen_tasks.insert(task.task_id).second)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate task {} in a task batch", task.task_id);
            readParameters(task.parameters, in);
            readExchangeStreams(task.input_exchange_streams, in);
            readExchangeStreams(task.output_exchange_streams, in);
            readStreamSources(stream_sources, version, in);
            readRuntimeFilterDescriptors(task.runtime_filter_descriptors, version, in);
        }
    }
}

void StatelessWorkerEndpoint::processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response)
{
    auto operation = params.get("operation");
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

        task_runner->startTasks(unique_query_id, std::move(batch), unique_temp_file_path, collectors);
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
        DistributedQueryTaskStatus task_status;
        task_status.progress = std::move(status.progress);
        task_status.logs = std::move(status.logs);

        switch (status.result)
        {
            case StatelessTaskExecutor::TaskRunnig:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Running";
                break;
            }
            case StatelessTaskExecutor::TaskFailing:
            {
                response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
                task_status.status = "Failing";
                task_status.error_message = status.message;
                task_status.error_code = status.error_code;
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
                task_status.error_code = status.error_code;
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
        task_status.write(out, status.collectors);
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
