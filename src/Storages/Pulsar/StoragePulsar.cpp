#include <Storages/Pulsar/StoragePulsar.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/Pulsar/PulsarLogger.h>
#include <Storages/Pulsar/PulsarProducer.h>
#include <Storages/Pulsar/PulsarSettings.h>
#include <Storages/Pulsar/PulsarSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_pulsar_storage_engine;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
    extern const SettingsBool use_concurrency_control;
}

namespace PulsarSetting
{
    extern const PulsarSettingsBool pulsar_commit_on_select;
    extern const PulsarSettingsMilliseconds pulsar_flush_interval_ms;
    extern const PulsarSettingsString pulsar_format;
    extern const PulsarSettingsString pulsar_group_name;
    extern const PulsarSettingsStreamingHandleErrorMode pulsar_handle_error_mode;
    extern const PulsarSettingsUInt64 pulsar_max_block_size;
    extern const PulsarSettingsUInt64 pulsar_max_rows_per_message;
    extern const PulsarSettingsUInt64 pulsar_num_consumers;
    extern const PulsarSettingsUInt64 pulsar_poll_max_batch_size;
    extern const PulsarSettingsMilliseconds pulsar_poll_timeout_ms;
    extern const PulsarSettingsString pulsar_schema;
    extern const PulsarSettingsString pulsar_service_url;
    extern const PulsarSettingsUInt64 pulsar_skip_broken_messages;
    extern const PulsarSettingsString pulsar_topic_list;
}

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int QUERY_NOT_ALLOWED;
extern const int ABORTED;
extern const int CANNOT_CONNECT_PULSAR;
extern const int SUPPORT_IS_DISABLED;
}

class ReadFromStoragePulsar final : public ReadFromStreamLikeEngine
{
public:
    ReadFromStoragePulsar(
        const Names & column_names_,
        StoragePtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info,
        ContextPtr context_)
        : ReadFromStreamLikeEngine(column_names_, storage_snapshot_, query_info.storage_limits, context_)
        , column_names(column_names_)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
    {
    }

    String getName() const override { return "ReadFromStoragePulsar"; }

private:
    Pipe makePipe() final
    {
        auto & pulsar_storage = storage->as<StoragePulsar &>();
        if (pulsar_storage.shutdown_called.load())
            throw Exception(ErrorCodes::ABORTED, "Table is detached");

        /// Check the dependencies directly instead of a flag set by the background task: the flag would be
        /// clear between the streaming cycles, letting a direct SELECT compete with (and, with
        /// `pulsar_commit_on_select = 1`, acknowledge messages behind) the attached views.
        if (!DatabaseCatalog::instance().getDependentViews(pulsar_storage.getStorageID()).empty())
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StoragePulsar with attached materialized views");

        /// The consumer pool may be incomplete: after ATTACH with the broker unreachable, or after
        /// a poisoned consumer was dropped, `init_task` is still recreating the missing consumers.
        /// Reading from an empty pool would return an empty result set, making a broker outage
        /// indistinguishable from an empty topic, so reject the read instead. Fan out only over
        /// the live consumers: a source for a missing slot would just wait `pulsar_max_wait_ms`
        /// for a consumer that cannot appear.
        size_t live_consumers = 0;
        {
            std::lock_guard lock{pulsar_storage.consumers_mutex};
            live_consumers = pulsar_storage.created_consumers;
        }
        if (live_consumers == 0)
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_PULSAR,
                "Pulsar consumers setup is not finished (0 out of {} consumers created), retrying in the background. "
                "Connection to the broker might not be established yet",
                pulsar_storage.num_consumers);

        /// Use all live consumers at once, otherwise SELECT may not read messages from all partitions.
        Pipes pipes;
        pipes.reserve(live_consumers);
        auto modified_context = pulsar_storage.addSettings(getContext());

        // Claim as many consumers as available, but don't block
        for (size_t i = 0; i < live_consumers; ++i)
            pipes.emplace_back(std::make_shared<PulsarSource>(
                pulsar_storage,
                storage_snapshot,
                modified_context,
                column_names,
                1,
                pulsar_storage.log,
                0,
                (*pulsar_storage.pulsar_settings)[PulsarSetting::pulsar_commit_on_select].value));

        return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

namespace
{

pulsar::ClientConfiguration createClientConfiguration()
{
    pulsar::ClientConfiguration config;
    /// Route the client library logs into the server logging (`setLogger` takes ownership).
    /// The default logger writes to stdout from the client's internal threads without synchronization.
    config.setLogger(new PulsarLoggerFactory());
    return config;
}

}

StoragePulsar::StoragePulsar(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    std::unique_ptr<PulsarSettings> pulsar_settings_,
    LoadingStrictnessLevel mode)
    : IStreamingStorage(table_id_)
    , WithContext(context_)
    , pulsar_settings(std::move(pulsar_settings_))
    , macros_info{.table_id = table_id_}
    , format_name(getContext()->getMacros()->expand((*pulsar_settings)[PulsarSetting::pulsar_format].value))
    , num_consumers((*pulsar_settings)[PulsarSetting::pulsar_num_consumers].value)
    , max_rows_per_message((*pulsar_settings)[PulsarSetting::pulsar_max_rows_per_message].value)
    , group_name(getContext()->getMacros()->expand((*pulsar_settings)[PulsarSetting::pulsar_group_name].value, macros_info))
    , schema_name(getContext()->getMacros()->expand((*pulsar_settings)[PulsarSetting::pulsar_schema].value, macros_info))
    , log(getLogger("Storage Pulsar(" + table_id_.table_name + ")"))
    , pulsar_client(
          getContext()->getMacros()->expand((*pulsar_settings)[PulsarSetting::pulsar_service_url].value, macros_info),
          createClientConfiguration())
    , topics(parseTopics(getContext()->getMacros()->expand((*pulsar_settings)[PulsarSetting::pulsar_topic_list].value, macros_info)))
    , semaphore(0, static_cast<int>(num_consumers))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    try
    {
        createConsumers();
    }
    catch (...)
    {
        /// A failure to subscribe must fail CREATE TABLE, but must not prevent the server
        /// from loading an already existing table when the broker is temporarily unreachable.
        /// The missing consumers are then recreated by `init_task` once the broker is back.
        if (mode <= LoadingStrictnessLevel::CREATE)
            throw;
        tryLogCurrentException(log, "Failed to subscribe to Pulsar topics on startup; will keep retrying in the background");
    }
    streamer = getContext()->getMessageBrokerSchedulePool()->createTask(getStorageID(), "PulsarStreamingTask", [this]() { streaming(); });
    streamer->deactivate();
    init_task = getContext()->getMessageBrokerSchedulePool()->createTask(getStorageID(), "PulsarInitTask", [this]() { initConsumersFunc(); });
    init_task->deactivate();
}

void StoragePulsar::startup()
{
    init_task->activateAndSchedule();
    streamer->activateAndSchedule();
}

void StoragePulsar::shutdown(bool /* is_drop */)
{
    shutdown_called.store(true);
    init_task->deactivate();
    streamer->deactivate();

    LOG_TRACE(log, "Closing Pulsar client");
    pulsar_client.close();
    LOG_TRACE(log, "Pulsar client closed");
}


void StoragePulsar::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    query_plan.addStep(
        std::make_unique<ReadFromStoragePulsar>(column_names, shared_from_this(), storage_snapshot, query_info, std::move(query_context)));
}

void StoragePulsar::pushConsumer(PulsarConsumerPtr consumer)
{
    std::lock_guard guard{consumers_mutex};
    consumers.push_back(std::move(consumer));
    semaphore.set();
}

PulsarConsumerPtr StoragePulsar::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}

PulsarConsumerPtr StoragePulsar::popConsumer(std::chrono::milliseconds timeout)
{
    // Wait for the first free consumer
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else if (!semaphore.tryWait(timeout.count()))
        return nullptr;

    // Take the first available consumer from the list
    std::lock_guard lock{consumers_mutex};
    auto consumer = consumers.back();
    consumers.pop_back();

    return consumer;
}

void StoragePulsar::returnConsumer(PulsarConsumerPtr consumer)
{
    if (consumer->isUsable())
    {
        pushConsumer(std::move(consumer));
        return;
    }

    /// The consumer hit a terminal receive error: re-pooling it would make every later cycle pop
    /// the same dead consumer and fail again. Drop it and let `init_task` recreate the slot.
    {
        std::lock_guard lock{consumers_mutex};
        --created_consumers;
    }
    consumer->consumer.close();
    LOG_WARNING(log, "Dropped a Pulsar consumer after a terminal receive error; a new one will be created");
    if (!shutdown_called.load())
        init_task->schedule();
}

void StoragePulsar::createConsumers()
{
    while (true)
    {
        {
            std::lock_guard lock{consumers_mutex};
            if (created_consumers >= num_consumers)
                return;
        }
        auto consumer = std::make_shared<PulsarConsumer>(log);
        createConsumer(consumer->consumer);
        {
            std::lock_guard lock{consumers_mutex};
            ++created_consumers;
        }
        pushConsumer(std::move(consumer));
    }
}

void StoragePulsar::initConsumersFunc()
{
    if (shutdown_called.load())
        return;

    try
    {
        createConsumers();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to create Pulsar consumers, will retry");
        if (!shutdown_called.load())
            init_task->scheduleAfter(PULSAR_RESCHEDULE_MS);
    }
}

SinkToStoragePtr
StoragePulsar::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto modified_context = addSettings(local_context);

    if (topics.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't write to Pulsar table with multiple topics!");

    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<PulsarProducer>(createProducer(), topics[0], shutdown_called, header);

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(
        std::make_shared<const Block>(header), getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}

ContextMutablePtr StoragePulsar::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    if ((*pulsar_settings)[PulsarSetting::pulsar_handle_error_mode] == StreamingHandleErrorMode::DEFAULT)
        modified_context->setSetting("input_format_allow_errors_num", (*pulsar_settings)[PulsarSetting::pulsar_skip_broken_messages].value);
    else if ((*pulsar_settings)[PulsarSetting::pulsar_handle_error_mode] == StreamingHandleErrorMode::DEAD_LETTER_QUEUE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DEAD_LETTER_QUEUE is not supported by the table engine");
    else
        modified_context->setSetting("input_format_allow_errors_num", Field{0});

    /// Since we are reusing the same context for all queries executed simultaneously, we don't want to used shared `analyze_count`
    modified_context->setSetting("max_analyze_depth", Field{0});

    if ((*pulsar_settings)[PulsarSetting::pulsar_schema].changed)
        modified_context->setSetting("format_schema", schema_name);

    /// Apply all other settings from the table definition (non-pulsar-related, e.g. format settings).
    modified_context->applySettingsChanges(pulsar_settings->getFormatSettings());

    /// It does not make sense to use auto detection here, since the format
    /// will be reset for each message, plus, auto detection takes CPU
    /// time.
    modified_context->setSetting("input_format_csv_detect_header", false);
    modified_context->setSetting("input_format_tsv_detect_header", false);
    modified_context->setSetting("input_format_custom_detect_header", false);

    return modified_context;
}

ProducerPtr StoragePulsar::createProducer()
{
    ProducerPtr producer = std::make_shared<pulsar::Producer>();
    pulsar::ProducerConfiguration config;
    size_t poll_timeout = getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
    config.setSendTimeout(static_cast<int>(poll_timeout));
    config.setBlockIfQueueFull(true);

    if (topics.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't write to Pulsar table with multiple topics!");

    auto result = pulsar_client.createProducer(topics[0], config, *producer);
    if (result != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_PULSAR,
            "Cannot create Pulsar producer for topic {}: {}",
            topics[0],
            pulsar::strResult(result));
    return producer;
}

void StoragePulsar::createConsumer(pulsar::Consumer & consumer)
{
    pulsar::ConsumerConfiguration config;
    config.setConsumerType(pulsar::ConsumerType::ConsumerShared);
    /// NOLINTNEXTLINE(google-runtime-int)
    config.setBatchReceivePolicy({static_cast<int>(getPollMaxBatchSize()), 0, static_cast<long>(getPollTimeoutMilliseconds())});

    auto result = pulsar_client.subscribe(topics, group_name, config, consumer);
    if (result != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_PULSAR,
            "Cannot subscribe to Pulsar topics [{}]: {}",
            fmt::join(topics, ", "),
            pulsar::strResult(result));
}

size_t StoragePulsar::getPollTimeoutMilliseconds() const
{
    return (*pulsar_settings)[PulsarSetting::pulsar_poll_timeout_ms].changed
        ? (*pulsar_settings)[PulsarSetting::pulsar_poll_timeout_ms].totalMilliseconds()
        : getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
}

size_t StoragePulsar::getPollMaxBatchSize() const
{
    size_t batch_size = (*pulsar_settings)[PulsarSetting::pulsar_poll_max_batch_size].changed
        ? (*pulsar_settings)[PulsarSetting::pulsar_poll_max_batch_size].value
        : getContext()->getSettingsRef()[Setting::max_block_size].value;

    /// A single `batchReceive` must not prefetch more than one block: a larger prefetch would leave
    /// an unread tail attached to the consumer, which an aborted SELECT then has to negative-ack.
    return std::min(batch_size, getMaxBlockSize());
}

size_t StoragePulsar::getMaxBlockSize() const
{
    return (*pulsar_settings)[PulsarSetting::pulsar_max_block_size].changed
        ? (*pulsar_settings)[PulsarSetting::pulsar_max_block_size].value
        : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}

StreamingHandleErrorMode StoragePulsar::getStreamingHandleErrorMode() const
{
    return (*pulsar_settings)[PulsarSetting::pulsar_handle_error_mode];
}

Names StoragePulsar::parseTopics(String topic_list) const
{
    Names result;
    boost::split(result, topic_list, [](char c) { return c == ','; });
    for (String & topic : result)
        boost::trim(topic);
    return result;
}

VirtualColumnsDescription StoragePulsar::createVirtuals()
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_topic", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_ordering_key", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_partition_key", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3)), "", VirtualsMaterializationPlace::Reader);

    if ((*pulsar_settings)[PulsarSetting::pulsar_handle_error_mode] == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
        desc.addEphemeral("_error", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}


bool StoragePulsar::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto view_ids = DatabaseCatalog::instance().getDependentViews(table_id);
    if (view_ids.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & view_id : view_ids)
    {
        auto view = DatabaseCatalog::instance().tryGetTable(view_id, getContext());
        if (!view)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(view.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(view_id))
            return false;
    }

    return true;
}

void StoragePulsar::scheduleStreamingTasksImpl()
{
    streamer->schedule();
}

void StoragePulsar::streaming()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
        const UInt64 cycle_epoch = stream_control.currentCancelEpoch();
        const bool deps_ready = num_views == 0 || checkDependencies(table_id);
        const bool run_cycle = deps_ready && stream_control.claimCycle(last_seen_refresh_epoch);

        if (num_views && run_cycle)
        {
            auto start_time = std::chrono::steady_clock::now();

            while (!shutdown_called.load())
            {
                if (!checkDependencies(table_id))
                    break;

                if (streamToViews(cycle_epoch))
                    break;

                if (stream_control.isBlocked() || stream_control.isCancelRequested(cycle_epoch))
                    break;

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
                if (duration.count() > PULSAR_MAX_THREAD_WORK_DURATION_MS)
                    break;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!shutdown_called.load())
        streamer->scheduleAfter(PULSAR_RESCHEDULE_MS);
}

bool StoragePulsar::streamToViews(UInt64 cycle_epoch)
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    /// The consumer pool may be incomplete: after ATTACH with the broker unreachable, or after
    /// a poisoned consumer was dropped, `init_task` is still recreating the missing consumers.
    /// A source for a missing slot would just spend the whole poll timeout waiting for a consumer
    /// that cannot appear, throttling the healthy sources, so fan out only over the live consumers
    /// and report a stall when there are none.
    size_t stream_count = 0;
    {
        std::lock_guard lock{consumers_mutex};
        stream_count = created_consumers;
    }
    if (stream_count == 0)
    {
        LOG_TRACE(log, "No consumers created yet (connection to the broker might not be established), skipping the streaming cycle");
        return true;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, getContext());

    // Create an INSERT query for streaming data
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto pulsar_context = addSettings(getContext());
    pulsar_context->makeQueryContext();

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        pulsar_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<PulsarSource>> sources;
    Pipes pipes;

    sources.reserve(stream_count);
    pipes.reserve(stream_count);
    for (size_t i = 0; i < stream_count; ++i)
    {
        Poco::Timespan max_execution_time = (*pulsar_settings)[PulsarSetting::pulsar_flush_interval_ms].changed
            ? (*pulsar_settings)[PulsarSetting::pulsar_flush_interval_ms]
            : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

        auto source = std::make_shared<PulsarSource>(
            *this,
            storage_snapshot,
            pulsar_context,
            block_io.pipeline.getHeader().getNames(),
            block_size,
            log,
            max_execution_time.totalMilliseconds(),
            /* commit_in_suffix */ false,
            cycle_epoch);
        sources.emplace_back(source);
        pipes.emplace_back(source);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    block_io.pipeline.complete(std::move(pipe));

    block_io.pipeline.setNumThreads(stream_count);
    block_io.pipeline.setConcurrencyControl(pulsar_context->getSettingsRef()[Setting::use_concurrency_control]);

    std::atomic_size_t rows = 0;
    block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
    CompletedPipelineExecutor executor(block_io.pipeline);
    try
    {
        executor.execute();
    }
    catch (...)
    {
        /// The blocks were not written: request redelivery of the polled messages
        /// instead of acknowledging them, to keep at-least-once delivery.
        for (auto & source : sources)
            source->rollback();
        throw;
    }

    LOG_TRACE(log, "Processed messages: {}", rows.load());

    if (stream_control.isCancelRequested(cycle_epoch))
    {
        /// The cycle was cancelled before its durable boundary: request redelivery of the polled
        /// messages instead of acknowledging them (blocks already written to the views will be
        /// delivered again - the usual at-least-once semantics, same as the exception path above).
        for (auto & source : sources)
            source->rollback();
        return true;
    }

    bool some_stream_is_stalled = false;
    for (auto & source : sources)
    {
        some_stream_is_stalled = some_stream_is_stalled || source->isStalled();
        /// The pipeline finished successfully, so the blocks are written to the views
        /// and the messages can be acknowledged.
        source->commit();
    }

    return some_stream_is_stalled;
}

void registerStoragePulsar(StorageFactory & factory);
void registerStoragePulsar(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        /// The check applies only to CREATE: existing tables must load on server startup
        /// and stay attachable regardless of the current value of the setting.
        if (args.mode <= LoadingStrictnessLevel::CREATE
            && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_pulsar_storage_engine])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Experimental Pulsar table engine is not enabled (the setting 'allow_experimental_pulsar_storage_engine')");
        }

        auto pulsar_settings = std::make_unique<PulsarSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
        {
            pulsar_settings->loadFromNamedCollection(named_collection);
        }
        else if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pulsar engine must have settings");

        if (args.storage_def->settings)
            pulsar_settings->loadFromQuery(*args.storage_def);

        if (!(*pulsar_settings)[PulsarSetting::pulsar_service_url].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_service_url` setting");

        if (!(*pulsar_settings)[PulsarSetting::pulsar_group_name].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_group_name` setting");

        if (!(*pulsar_settings)[PulsarSetting::pulsar_format].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_format` setting");

        if (!(*pulsar_settings)[PulsarSetting::pulsar_topic_list].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_topic_list` setting");

        /// A table with zero consumers could never consume anything, and `num_consumers`
        /// is used as a divisor in `getMaxBlockSize`.
        if ((*pulsar_settings)[PulsarSetting::pulsar_num_consumers].value < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The setting `pulsar_num_consumers` must be at least 1");

        /// A zero block size would stop `PulsarSource` after its first (empty) loop iteration,
        /// and a zero poll batch size would be passed straight into `setBatchReceivePolicy`.
        if ((*pulsar_settings)[PulsarSetting::pulsar_max_block_size].changed
            && (*pulsar_settings)[PulsarSetting::pulsar_max_block_size].value < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The setting `pulsar_max_block_size` must be at least 1");

        if ((*pulsar_settings)[PulsarSetting::pulsar_poll_max_batch_size].changed
            && (*pulsar_settings)[PulsarSetting::pulsar_poll_max_batch_size].value < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The setting `pulsar_poll_max_batch_size` must be at least 1");

        /// The mode is accepted by the generic `StreamingHandleErrorMode` parser but not implemented
        /// by this engine, so reject it up front instead of failing on the first broken message.
        if (args.mode <= LoadingStrictnessLevel::CREATE
            && (*pulsar_settings)[PulsarSetting::pulsar_handle_error_mode] == StreamingHandleErrorMode::DEAD_LETTER_QUEUE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`dead_letter_queue` mode of `pulsar_handle_error_mode` is not supported by the table engine");

        return std::make_shared<StoragePulsar>(args.table_id, args.getContext(), args.columns, std::move(pulsar_settings), args.mode);
    };

    factory.registerStorage(
        "Pulsar",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .source_access_type = AccessTypeObjects::Source::PULSAR,
            .has_builtin_setting_fn = PulsarSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine allows integrating ClickHouse with [Apache Pulsar](https://pulsar.apache.org/).

`Pulsar` lets you:

- Subscribe to one or more Pulsar topics and publish to a single Pulsar topic (`INSERT` is supported only for tables with exactly one topic).
- Process new messages as they become available.

The engine is experimental. To create a table with it, the setting `allow_experimental_pulsar_storage_engine` must be enabled.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Pulsar SETTINGS
    pulsar_service_url = 'pulsar://host:port',
    pulsar_topic_list = 'topic1,topic2,...',
    pulsar_group_name = 'group_name',
    pulsar_format = 'data_format'[,]
    [pulsar_schema = '',]
    [pulsar_num_consumers = N,]
    [pulsar_max_block_size = N,]
    [pulsar_skip_broken_messages = N,]
    [pulsar_poll_timeout_ms = N,]
    [pulsar_poll_max_batch_size = N,]
    [pulsar_flush_interval_ms = N,]
    [pulsar_handle_error_mode = 'default',]
    [pulsar_max_rows_per_message = 1,]
    [pulsar_commit_on_select = false]
```

Required parameters:

- `pulsar_service_url` – The Pulsar broker URL, for example, `pulsar://localhost:6650`.
- `pulsar_group_name` – The subscription name. All consumers sharing the same group name belong to the same subscription.
- `pulsar_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](/reference/formats/index) section.
- `pulsar_topic_list` – A comma-separated list of Pulsar topics to consume from. Writing via `INSERT` is supported only when the list contains exactly one topic; an `INSERT` into a table with multiple topics throws `NOT_IMPLEMENTED`.

Optional parameters:

- `pulsar_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `pulsar_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient.
- `pulsar_max_block_size` – The maximum batch size (in messages) for a poll. Default: [max_insert_block_size](/reference/settings/session-settings/max-insert#max_insert_block_size).
- `pulsar_skip_broken_messages` – Parser tolerance to schema-incompatible messages per block. Default: `0`. If `pulsar_skip_broken_messages = N` then the engine skips *N* Pulsar messages that cannot be parsed (a message equals a row of data).
- `pulsar_poll_timeout_ms` – Timeout for a single poll from Pulsar. Default: [stream_poll_timeout_ms](/reference/settings/session-settings/stream#stream_poll_timeout_ms).
- `pulsar_poll_max_batch_size` – The maximum number of messages to be polled in a single Pulsar poll. Default: [max_block_size](/reference/settings/session-settings/max-block-size#max_block_size).
- `pulsar_flush_interval_ms` – Timeout for flushing data read from Pulsar. Default: [stream_flush_interval_ms](/reference/settings/session-settings/stream#stream_flush_interval_ms).
- `pulsar_handle_error_mode` – How to handle errors for the Pulsar engine. Possible values: `default` (the exception will be thrown if we fail to parse a message), `stream` (the exception message and raw message will be saved in the virtual columns `_error` and `_raw_message`).
- `pulsar_max_rows_per_message` – The maximum number of rows written in one Pulsar message for row-based formats. Default: `1`.
- `pulsar_commit_on_select` – Acknowledge polled messages when a direct `SELECT` query is made from the table. Default: `false`.

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](/reference/statements/create/view). To do this:

1. Use the engine to create a Pulsar consumer and consider it a data stream.
2. Create a table with the desired structure.
3. Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from Pulsar and convert them to the required format using `SELECT`.
One Pulsar table can have as many materialized views as you like; they do not read data from the table directly, but receive new records (in blocks). This way you can write to several tables with different detail levels (with grouping - aggregation and without).

Example:

```sql
CREATE TABLE queue
(
    key UInt64,
    value UInt64
) ENGINE = Pulsar
  SETTINGS pulsar_service_url = 'pulsar://localhost:6650',
           pulsar_topic_list = 'topic1',
           pulsar_group_name = 'group1',
           pulsar_format = 'JSONEachRow';

CREATE TABLE daily
(
    key UInt64,
    value UInt64
) ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

SELECT key, value FROM daily ORDER BY key;
```

To stop receiving streamed data or to change the conversion logic, detach the materialized view:

```sql
DETACH TABLE consumer;
ATTACH TABLE consumer;
```

## Virtual columns {#virtual-columns}

- `_topic` – Pulsar topic. Data type: `LowCardinality(String)`.
- `_ordering_key` – The ordering key of the message. Data type: `String`.
- `_partition_key` – The partition key of the message. Data type: `String`.
- `_timestamp` – The event timestamp of the message. Data type: `Nullable(DateTime)`.
- `_timestamp_ms` – The event timestamp of the message in milliseconds. Data type: `Nullable(DateTime64(3))`.

Additional virtual columns when `pulsar_handle_error_mode = 'stream'`:

- `_raw_message` – Raw message that could not be parsed successfully. Data type: `String`.
- `_error` – Exception message happened during failed parsing. Data type: `String`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of an exception during parsing; they are always empty when the message was parsed successfully.
)DOCS_MD",
            .syntax
            = "ENGINE = Pulsar() SETTINGS pulsar_service_url = 'pulsar://host:port', pulsar_topic_list = 'topic', pulsar_group_name = "
              "'group', pulsar_format = 'format', ...",
            .related = {"Kafka", "NATS", "RabbitMQ"}});
}

}
