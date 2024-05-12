#include <Storages/Pulsar/StoragePulsar.h>

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
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/Pulsar/PulsarProducer.h>
#include <Storages/Pulsar/PulsarSettings.h>
#include <Storages/Pulsar/PulsarSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int QUERY_NOT_ALLOWED;
extern const int ABORTED;
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

        if (pulsar_storage.mv_attached)
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StoragePulsar with attached materialized views");

        /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
        Pipes pipes;
        pipes.reserve(pulsar_storage.num_consumers);
        auto modified_context = Context::createCopy(getContext());

        // Claim as many consumers as requested, but don't block
        for (size_t i = 0; i < pulsar_storage.num_consumers; ++i)
            pipes.emplace_back(std::make_shared<PulsarSource>(pulsar_storage, storage_snapshot, modified_context, column_names, 1, pulsar_storage.log, 0));

        return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

StoragePulsar::StoragePulsar(
    const StorageID & table_id_, ContextPtr context_, const ColumnsDescription & columns_, std::unique_ptr<PulsarSettings> pulsar_settings_)
    : IStorage(table_id_)
    , WithContext(context_)
    , pulsar_settings(std::move(pulsar_settings_))
    , format_name(pulsar_settings->pulsar_format.value)
    , num_consumers(pulsar_settings->pulsar_num_consumers.value)
    , max_rows_per_message(pulsar_settings->pulsar_max_rows_per_message.value)
    , log(getLogger("Storage Pulsar(" + table_id_.table_name + ")"))
    , pulsar_client(pulsar_settings->pulsar_broker_address.value)
    , topics(parseTopics(pulsar_settings->pulsar_topic_list.value))
    , semaphore(0, static_cast<int>(num_consumers))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(createVirtuals());

    for (size_t i = 0; i < num_consumers; ++i)
    {
        auto consumer = std::make_shared<PulsarConsumer>(log);
        createConsumer(consumer->consumer);
        pushConsumer(consumer);
    }
    streamer = getContext()->getMessageBrokerSchedulePool().createTask("Storage Pulsar", [this]() { streaming(); });
    streamer->deactivate();
}

void StoragePulsar::startup()
{
    streamer->activateAndSchedule();
}

void StoragePulsar::shutdown(bool /* is_drop */)
{
    shutdown_called.store(true);
    for (size_t i = 0; i < num_consumers; ++i)
        popConsumer()->consumer.close();
    pulsar_client.close();
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

SinkToStoragePtr
StoragePulsar::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto modified_context = Context::createCopy(local_context);

    if (topics.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't write to Pulsar table with multiple topics!");

    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<PulsarProducer>(createProducer(), topics[0], shutdown_called, header);

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef().output_format_avro_rows_in_file.changed)
        max_rows = local_context->getSettingsRef().output_format_avro_rows_in_file.value;
    return std::make_shared<MessageQueueSink>(header, getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}

ProducerPtr StoragePulsar::createProducer()
{
    ProducerPtr producer = std::make_shared<pulsar::Producer>();
    pulsar::ProducerConfiguration config;
    size_t poll_timeout = getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
    config.setSendTimeout(static_cast<int>(poll_timeout));
    config.setBlockIfQueueFull(true);

    chassert(topics.size() == 1);

    pulsar_client.createProducer(topics[0], config, *producer);
    return producer;
}

void StoragePulsar::createConsumer(pulsar::Consumer & consumer)
{
    pulsar::ConsumerConfiguration config;
    config.setConsumerType(pulsar::ConsumerType::ConsumerShared);
    config.setBatchReceivePolicy({0, 0, static_cast<long>(getPollTimeoutMilliseconds())});

    pulsar_client.subscribe(topics, "clickhouse", config, consumer);
}

size_t StoragePulsar::getPollTimeoutMilliseconds() const
{
    return pulsar_settings->pulsar_poll_timeout_ms.changed ? pulsar_settings->pulsar_poll_timeout_ms.totalMilliseconds()
                                                           : getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}

size_t StoragePulsar::getPollMaxBatchSize() const
{
    return pulsar_settings->pulsar_poll_max_batch_size.changed ? pulsar_settings->pulsar_poll_max_batch_size.value
                                                               : getContext()->getSettingsRef().max_block_size.value;
}

size_t StoragePulsar::getMaxBlockSize() const
{
    return pulsar_settings->pulsar_max_block_size.changed ? pulsar_settings->pulsar_max_block_size.value
                                                          : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}

StreamingHandleErrorMode StoragePulsar::getStreamingHandleErrorMode() const
{
    return pulsar_settings->pulsar_handle_error_mode;
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

    desc.addEphemeral("_topic", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    desc.addEphemeral("_ordering_key", std::make_shared<DataTypeString>(), "");
    desc.addEphemeral("_partition_key", std::make_shared<DataTypeString>(), "");
    desc.addEphemeral("_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "");
    desc.addEphemeral("_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3)), "");

    if (pulsar_settings->pulsar_handle_error_mode.value == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeString>(), "");
        desc.addEphemeral("_error", std::make_shared<DataTypeString>(), "");
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

void StoragePulsar::streaming()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
        if (num_views)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);

            while (!stream_cancelled.load())
            {
                if (!checkDependencies(table_id))
                    break;


                streamToViews();

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
                if (duration.count() > PULSAR_MAX_THREAD_WORK_DURATION_MS)
                    break;
            }
        }
    }
    catch (...)
    {
    }

    mv_attached.store(false);

    // Wait for attached views
    if (!stream_cancelled.load())
        streamer->scheduleAfter(PULSAR_RESCHEDULE_MS);
}

void StoragePulsar::streamToViews()
{
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto pulsar_context = Context::createCopy(getContext());
    pulsar_context->makeQueryContext();

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, pulsar_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<PulsarSource>> sources;
    Pipes pipes;

    size_t stream_count = 1;
    sources.reserve(stream_count);
    pipes.reserve(stream_count);
    for (size_t i = 0; i < stream_count; ++i)
    {
        Poco::Timespan max_execution_time = pulsar_settings->pulsar_flush_interval_ms.changed
            ? pulsar_settings->pulsar_flush_interval_ms
            : getContext()->getSettingsRef().stream_flush_interval_ms;

        auto source = std::make_shared<PulsarSource>(
            *this,
            storage_snapshot,
            pulsar_context,
            block_io.pipeline.getHeader().getNames(),
            block_size,
            log,
            max_execution_time.milliseconds());
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(std::move(pipe));

        // we need to read all consumers in parallel (sequential read may lead to situation
        // when some of consumers are not used, and will break some Kafka consumer invariants)
        block_io.pipeline.setNumThreads(stream_count);
        block_io.pipeline.setConcurrencyControl(pulsar_context->getSettingsRef().use_concurrency_control);

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
}

void registerStoragePulsar(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto pulsar_settings = std::make_unique<PulsarSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext()))
        {
            for (const auto & setting : pulsar_settings->all())
            {
                const auto & setting_name = setting.getName();
                if (named_collection->has(setting_name))
                    pulsar_settings->set(setting_name, named_collection->get<String>(setting_name));
            }
        }
        else if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pulsar engine must have settings");

        if (args.storage_def->settings)
            pulsar_settings->loadFromQuery(*args.storage_def);

        if (!pulsar_settings->pulsar_broker_address.changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_broker_address` settings");

        if (!pulsar_settings->pulsar_format.changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `pulsar_format` setting");

        return std::make_shared<StoragePulsar>(args.table_id, args.getContext(), args.columns, std::move(pulsar_settings));
    };

    factory.registerStorage(
        "Pulsar",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

}
