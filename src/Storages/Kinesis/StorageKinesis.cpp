#include <Storages/Kinesis/StorageKinesis.h>
#include <Storages/Kinesis/KinesisConsumer.h>
#include <Storages/Kinesis/KinesisSource.h>
#include <Storages/Kinesis/KinesisSink.h>
#include <Storages/Kinesis/KinesisSettings.h>

#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Core/StreamingHandleErrorMode.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <IO/S3/Client.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisEndpointProvider.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ListShardsResult.h>
#include <aws/kinesis/model/ShardIteratorType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_KINESIS;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
}

namespace KinesisSetting
{
    extern const KinesisSettingsString kinesis_aws_access_key_id;
    extern const KinesisSettingsString kinesis_aws_secret_access_key;
    extern const KinesisSettingsString kinesis_aws_region;
    extern const KinesisSettingsString kinesis_endpoint;
    extern const KinesisSettingsString kinesis_format;
    extern const KinesisSettingsMilliseconds kinesis_flush_interval_ms;
    extern const KinesisSettingsStreamingHandleErrorMode kinesis_handle_error_mode;
    extern const KinesisSettingsUInt64 kinesis_max_block_size;
    extern const KinesisSettingsUInt64 kinesis_max_records_per_request;
    extern const KinesisSettingsUInt64 kinesis_max_rows_per_message;
    extern const KinesisSettingsUInt64 kinesis_num_consumers;
    extern const KinesisSettingsUInt64 kinesis_poll_timeout_ms;
    extern const KinesisSettingsBool kinesis_save_checkpoints;
    extern const KinesisSettingsString kinesis_schema;
    extern const KinesisSettingsUInt64 kinesis_skip_broken_messages;
    extern const KinesisSettingsString kinesis_starting_position;
    extern const KinesisSettingsUInt64 kinesis_at_timestamp;
    extern const KinesisSettingsString kinesis_stream_name;
    extern const KinesisSettingsBool kinesis_verify_ssl;
}

VirtualColumnsDescription StorageKinesis::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_sequence_number", std::make_shared<DataTypeString>(), "Kinesis sequence number");
    desc.addEphemeral("_partition_key", std::make_shared<DataTypeString>(), "Kinesis partition key");
    desc.addEphemeral("_shard_id", std::make_shared<DataTypeString>(), "Kinesis shard ID");
    desc.addEphemeral("_approximate_arrival_timestamp", std::make_shared<DataTypeUInt64>(), "Unix timestamp (seconds) when the record arrived in the stream");
    return desc;
}

static Aws::Kinesis::Model::ShardIteratorType parseStartingPosition(const String & s)
{
    if (s == "LATEST")
        return Aws::Kinesis::Model::ShardIteratorType::LATEST;
    if (s == "TRIM_HORIZON")
        return Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON;
    if (s == "AT_TIMESTAMP")
        return Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown kinesis_starting_position '{}'. Valid values: LATEST, TRIM_HORIZON, AT_TIMESTAMP", s);
}

std::shared_ptr<Aws::Kinesis::KinesisClient> StorageKinesis::createClient() const
{
    auto & _ = DB::S3::ClientFactory::instance();

    Aws::Client::ClientConfiguration config;
    config.region = (*kinesis_settings)[KinesisSetting::kinesis_aws_region].value;
    config.verifySSL = (*kinesis_settings)[KinesisSetting::kinesis_verify_ssl].value;
    config.scheme = Aws::Http::Scheme::HTTP;

    const String & endpoint = (*kinesis_settings)[KinesisSetting::kinesis_endpoint].value;
    if (!endpoint.empty())
        config.endpointOverride = endpoint;

    const String & access_key = (*kinesis_settings)[KinesisSetting::kinesis_aws_access_key_id].value;
    const String & secret_key = (*kinesis_settings)[KinesisSetting::kinesis_aws_secret_access_key].value;

    if (!access_key.empty() && !secret_key.empty())
    {
        Aws::Auth::AWSCredentials credentials(access_key, secret_key);
        return std::make_shared<Aws::Kinesis::KinesisClient>(
            credentials,
            Aws::MakeShared<Aws::Kinesis::KinesisEndpointProvider>("KinesisEndpointProvider"),
            config);
    }

    return std::make_shared<Aws::Kinesis::KinesisClient>(
        std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>(),
        Aws::MakeShared<Aws::Kinesis::KinesisEndpointProvider>("KinesisEndpointProvider"),
        config);
}

StorageKinesis::StorageKinesis(
    const StorageID & table_id,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<KinesisSettings> kinesis_settings_)
    : IStorage(table_id)
    , WithContext(context_)
    , kinesis_settings(std::move(kinesis_settings_))
    , log(getLogger("StorageKinesis(" + table_id.table_name + ")"))
    , semaphore(0, static_cast<int>((*kinesis_settings)[KinesisSetting::kinesis_num_consumers].value))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(createVirtuals());

    if ((*kinesis_settings)[KinesisSetting::kinesis_stream_name].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "kinesis_stream_name setting is required for storage Kinesis");

    if ((*kinesis_settings)[KinesisSetting::kinesis_handle_error_mode] == StreamingHandleErrorMode::STREAM)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "kinesis_handle_error_mode = STREAM is not supported for Kinesis storage");

    try
    {
        client = createClient();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to create Kinesis client");
        throw;
    }
}

StorageKinesis::~StorageKinesis() = default;

void StorageKinesis::startup()
{
    if (shutdown_called)
        return;

    const bool save_checkpoints = (*kinesis_settings)[KinesisSetting::kinesis_save_checkpoints].value;

    std::map<String, KinesisShardState> checkpoints;
    if (save_checkpoints)
    {
        try
        {
            createCheckpointTableIfNeeded();
            checkpoints = loadCheckpoints();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to load Kinesis checkpoints");
        }
    }

    auto shard_assignments = listAndDistributeShards(checkpoints);
    const size_t num = shard_assignments.size();

    for (size_t i = 0; i < num; ++i)
    {
        auto consumer = createConsumer(std::move(shard_assignments[i]));
        consumers_ref.push_back(consumer);
        pushConsumer(consumer);
    }

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask(
        getStorageID(), "KinesisStreamingToViews", [this]() { streamingToViewsFunc(); });
    streaming_task->activateAndSchedule();
}

void StorageKinesis::shutdown(bool /*is_drop*/)
{
    if (shutdown_called.exchange(true))
        return;

    for (auto & weak_consumer : consumers_ref)
    {
        if (auto consumer = weak_consumer.lock())
            consumer->stop();
    }

    if (streaming_task)
        streaming_task->deactivate();

    const size_t num = (*kinesis_settings)[KinesisSetting::kinesis_num_consumers].value;
    for (size_t i = 0; i < num; ++i)
        popConsumer(std::chrono::milliseconds(0));

    client.reset();
}

std::vector<std::map<String, KinesisShardState>> StorageKinesis::listAndDistributeShards(
    const std::map<String, KinesisShardState> & checkpoints) const
{
    const size_t num_consumers = (*kinesis_settings)[KinesisSetting::kinesis_num_consumers].value;
    const String & stream_name = (*kinesis_settings)[KinesisSetting::kinesis_stream_name].value;

    std::vector<String> shard_ids;
    String next_token;

    do
    {
        Aws::Kinesis::Model::ListShardsRequest request;

        if (next_token.empty())
            request.SetStreamName(stream_name);
        else
            request.SetNextToken(next_token);

        auto outcome = client->ListShards(request);
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_KINESIS,
                "Failed to list shards for stream {}: {} ({})",
                stream_name, error.GetMessage(), error.GetExceptionName());
        }

        for (const auto & shard : outcome.GetResult().GetShards())
        {
            if (!shard.GetSequenceNumberRange().GetEndingSequenceNumber().empty())
                continue;
            shard_ids.push_back(shard.GetShardId());
        }

        next_token = outcome.GetResult().GetNextToken();
    }
    while (!next_token.empty());

    if (shard_ids.empty())
    {
        LOG_WARNING(log, "No open shards found in Kinesis stream {}", stream_name);
        return std::vector<std::map<String, KinesisShardState>>(num_consumers);
    }

    LOG_DEBUG(log, "Found {} open shards in stream {}, distributing among {} consumers",
        shard_ids.size(), stream_name, num_consumers);

    /// Round-robin
    std::vector<std::map<String, KinesisShardState>> assignments(num_consumers);
    for (size_t i = 0; i < shard_ids.size(); ++i)
    {
        const String & shard_id = shard_ids[i];
        KinesisShardState state;

        auto it = checkpoints.find(shard_id);
        if (it != checkpoints.end())
            state = it->second;

        assignments[i % num_consumers].emplace(shard_id, std::move(state));
    }

    return assignments;
}

KinesisConsumerPtr StorageKinesis::createConsumer(std::map<String, KinesisShardState> shard_states) const
{
    return std::make_shared<KinesisConsumer>(
        (*kinesis_settings)[KinesisSetting::kinesis_stream_name].value,
        client,
        std::move(shard_states),
        (*kinesis_settings)[KinesisSetting::kinesis_max_records_per_request].value,
        parseStartingPosition((*kinesis_settings)[KinesisSetting::kinesis_starting_position].value),
        (*kinesis_settings)[KinesisSetting::kinesis_at_timestamp].value,
        1000 /* internal_queue_size */);
}

void StorageKinesis::pushConsumer(KinesisConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumers.push_back(std::move(consumer));
    semaphore.set();
}

KinesisConsumerPtr StorageKinesis::popConsumer()
{
    semaphore.wait();
    std::lock_guard lock(mutex);
    auto consumer = std::move(consumers.back());
    consumers.pop_back();
    return consumer;
}

KinesisConsumerPtr StorageKinesis::popConsumer(std::chrono::milliseconds timeout)
{
    if (timeout == std::chrono::milliseconds::zero())
    {
        if (!semaphore.tryWait(0))
            return nullptr;
    }
    else
    {
        if (!semaphore.tryWait(static_cast<long>(timeout.count())))
            return nullptr;
    }

    std::lock_guard lock(mutex);
    if (consumers.empty())
        return nullptr;
    auto consumer = std::move(consumers.back());
    consumers.pop_back();
    return consumer;
}

void StorageKinesis::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_KINESIS, "Kinesis storage is shutting down");

    const size_t num_consumers = (*kinesis_settings)[KinesisSetting::kinesis_num_consumers].value;
    const size_t streams = std::min(num_streams, num_consumers);

    const UInt64 flush_ms = (*kinesis_settings)[KinesisSetting::kinesis_flush_interval_ms].totalMilliseconds();
    const UInt64 max_execution_ms = flush_ms
        ? flush_ms
        : static_cast<UInt64>(local_context->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    Pipes pipes;
    pipes.reserve(streams);
    for (size_t i = 0; i < streams; ++i)
    {
        pipes.emplace_back(std::make_shared<KinesisSource>(
            *this,
            storage_snapshot,
            (*kinesis_settings)[KinesisSetting::kinesis_format].value,
            storage_snapshot->getSampleBlockForColumns(column_names),
            max_block_size ? max_block_size : local_context->getSettingsRef()[Setting::max_block_size].value,
            max_execution_ms,
            local_context,
            (*kinesis_settings)[KinesisSetting::kinesis_skip_broken_messages].value > 0,
            (*kinesis_settings)[KinesisSetting::kinesis_skip_broken_messages].value));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, local_context, shared_from_this());
}

SinkToStoragePtr StorageKinesis::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /*async_insert*/)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_KINESIS, "Kinesis storage is shutting down");

    return std::make_shared<KinesisSink>(
        metadata_snapshot,
        client,
        (*kinesis_settings)[KinesisSetting::kinesis_stream_name].value,
        (*kinesis_settings)[KinesisSetting::kinesis_format].value,
        (*kinesis_settings)[KinesisSetting::kinesis_max_rows_per_message].value,
        local_context);
}

bool StorageKinesis::tryStreamToViews()
{
    if (shutdown_called)
        return false;

    if (!connection_ok)
    {
        try
        {
            client = createClient();
            connection_ok = true;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to recreate Kinesis client");
            return false;
        }
    }

    auto table_id = getStorageID();
    auto view_dependencies = DatabaseCatalog::instance().getDependentViews(table_id);
    if (view_dependencies.empty())
        return false;

    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;

    auto insert_context = Context::createCopy(getContext());
    insert_context->makeQueryContext();
    InterpreterInsertQuery interpreter(
        insert,
        insert_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);

    auto block_io = interpreter.execute();

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    auto column_names = block_io.pipeline.getHeader().getNames();
    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    const size_t num_consumers = (*kinesis_settings)[KinesisSetting::kinesis_num_consumers].value;

    const UInt64 flush_ms = (*kinesis_settings)[KinesisSetting::kinesis_flush_interval_ms].totalMilliseconds();
    const UInt64 max_execution_ms = flush_ms
        ? flush_ms
        : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    std::vector<std::shared_ptr<KinesisSource>> sources;
    Pipes pipes;
    sources.reserve(num_consumers);
    pipes.reserve(num_consumers);

    for (size_t i = 0; i < num_consumers; ++i)
    {
        auto source = std::make_shared<KinesisSource>(
            *this,
            storage_snapshot,
            (*kinesis_settings)[KinesisSetting::kinesis_format].value,
            sample_block,
            (*kinesis_settings)[KinesisSetting::kinesis_max_block_size].value
                ? (*kinesis_settings)[KinesisSetting::kinesis_max_block_size].value
                : getContext()->getSettingsRef()[Setting::max_block_size].value,
            max_execution_ms,
            getContext(),
            (*kinesis_settings)[KinesisSetting::kinesis_skip_broken_messages].value > 0,
            (*kinesis_settings)[KinesisSetting::kinesis_skip_broken_messages].value);

        sources.emplace_back(source);
        pipes.emplace_back(source);
    }

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    bool write_failed = false;
    try
    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    catch (...)
    {
        write_failed = true;
        connection_ok = false;
        tryLogCurrentException(log, "Failed to push Kinesis records to views");
    }

    if (!write_failed && (*kinesis_settings)[KinesisSetting::kinesis_save_checkpoints].value)
    {
        std::map<String, KinesisShardState> all_states;
        for (const auto & source : sources)
        {
            if (auto consumer = source->getConsumer())
            {
                for (auto & [shard_id, state] : consumer->getShardStates())
                    all_states[shard_id] = state;
            }
        }

        try
        {
            saveCheckpoints(all_states);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to save Kinesis checkpoints");
        }
    }

    return !write_failed;
}

void StorageKinesis::scheduleNextExecution(bool success)
{
    if (!streaming_task)
        return;

    if (success)
        streaming_task->schedule();
    else
        streaming_task->scheduleAfter((*kinesis_settings)[KinesisSetting::kinesis_poll_timeout_ms].value);
}

void StorageKinesis::streamingToViewsFunc()
{
    if (shutdown_called)
        return;

    bool success = false;
    try
    {
        success = tryStreamToViews();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in Kinesis streaming task");
    }

    scheduleNextExecution(success);
}

void StorageKinesis::createCheckpointTableIfNeeded() const
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    auto thread_group = ThreadGroup::createForQuery(query_context);
    ThreadGroupSwitcher switcher(thread_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);

    String create_query = R"(
        CREATE TABLE IF NOT EXISTS system.kinesis_checkpoints
        (
            stream_name String,
            shard_id String,
            last_sequence String,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (stream_name, shard_id)
    )";

    auto [ast, io] = DB::executeQuery(create_query, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete);
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
}

std::map<String, KinesisShardState> StorageKinesis::loadCheckpoints() const
{
    const String & stream_name = (*kinesis_settings)[KinesisSetting::kinesis_stream_name].value;

    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    auto thread_group_load = ThreadGroup::createForQuery(query_context);
    ThreadGroupSwitcher switcher_load(thread_group_load, ThreadName::UNKNOWN, /*allow_existing_group=*/true);

    String select_query = fmt::format(
        "SELECT shard_id, last_sequence FROM system.kinesis_checkpoints "
        "FINAL WHERE stream_name = '{}'",
        stream_name);

    auto [ast, io] = DB::executeQuery(select_query, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete);

    std::map<String, KinesisShardState> result;

    if (!io.pipeline.initialized())
        return result;

    auto pipeline = std::move(io.pipeline);
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        const auto & shard_col = block.getByName("shard_id").column;
        const auto & seq_col = block.getByName("last_sequence").column;

        for (size_t r = 0; r < block.rows(); ++r)
        {
            String shard_id{shard_col->getDataAt(r)};
            String last_seq{seq_col->getDataAt(r)};

            KinesisShardState state;
            state.last_sequence = last_seq;
            result.emplace(std::move(shard_id), std::move(state));
        }
    }

    LOG_DEBUG(log, "Loaded {} checkpoint(s) for stream {}", result.size(), stream_name);
    return result;
}

void StorageKinesis::saveCheckpoints(const std::map<String, KinesisShardState> & states) const
{
    if (states.empty())
        return;

    const String & stream_name = (*kinesis_settings)[KinesisSetting::kinesis_stream_name].value;

    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    auto thread_group_save = ThreadGroup::createForQuery(query_context);
    ThreadGroupSwitcher switcher_save(thread_group_save, ThreadName::UNKNOWN, /*allow_existing_group=*/true);

    String insert_query = "INSERT INTO system.kinesis_checkpoints (stream_name, shard_id, last_sequence) VALUES ";
    bool first = true;
    for (const auto & [shard_id, state] : states)
    {
        if (state.last_sequence.empty())
            continue;

        if (!first)
            insert_query += ", ";
        insert_query += fmt::format("('{}', '{}', '{}')", stream_name, shard_id, state.last_sequence);
        first = false;
    }

    if (first)
        return;

    auto [ast, io] = DB::executeQuery(insert_query, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete);
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
}

void registerStorageKinesis(StorageFactory & factory)
{
    factory.registerStorage(
        "Kinesis",
        [](const StorageFactory::Arguments & args)
        {
            auto kinesis_settings = std::make_unique<KinesisSettings>();
            if (args.storage_def->settings)
                kinesis_settings->loadFromQuery(*args.storage_def);

            if ((*kinesis_settings)[KinesisSetting::kinesis_stream_name].value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Kinesis storage requires kinesis_stream_name setting");

            if ((*kinesis_settings)[KinesisSetting::kinesis_format].value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Kinesis storage requires kinesis_format setting");

            return std::make_shared<StorageKinesis>(
                args.table_id,
                args.getContext(),
                args.columns,
                args.comment,
                std::move(kinesis_settings));
        },
        {
            .supports_settings = true,
            .supports_skipping_indices = false,
            .supports_projections = false,
            .supports_sort_order = false,
            .supports_ttl = false,
            .supports_replication = false,
            .supports_deduplication = false,
            .supports_parallel_insert = false,
            .supports_schema_inference = false,
            .source_access_type = AccessTypeObjects::Source::KINESIS,
            .has_builtin_setting_fn = KinesisSettings::hasBuiltin,
        });
}

}

#endif // USE_AWS_KINESIS
