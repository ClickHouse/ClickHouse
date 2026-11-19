#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out)
{
    serializeTaskBatch(batch, out, [&out](const SerializedQueryPlanPtr & plan) { out.write(plan->data(), plan->size()); });
}

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out, const TaskBatchPlanWriter & write_plan)
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
        writeVarUInt(stage.serialized_query_plan->size(), out);
        write_plan(stage.serialized_query_plan);
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

}
