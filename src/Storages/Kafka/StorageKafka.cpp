#include <Storages/Kafka/StorageKafka.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <Storages/Kafka/KafkaConfigLoader.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/KafkaSource.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <cppkafka/configuration.h>
#include <librdkafka/rdkafka.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>

#include <Storages/ColumnDefault.h>
#include <Common/config_version.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <base/sleep.h>

namespace CurrentMetrics
{
    extern const Metric KafkaBackgroundReads;
    extern const Metric KafkaConsumersInUse;
    extern const Metric KafkaWrites;
}

namespace ProfileEvents
{
    extern const Event KafkaDirectReads;
    extern const Event KafkaBackgroundReads;
    extern const Event KafkaWrites;
}


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
    extern const SettingsBool use_concurrency_control;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int ABORTED;
}

class ReadFromStorageKafka final : public ReadFromStreamLikeEngine
{
public:
    ReadFromStorageKafka(
        const Names & column_names_,
        StoragePtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info,
        ContextPtr context_)
        : ReadFromStreamLikeEngine{column_names_, storage_snapshot_, query_info.storage_limits, context_}
        , column_names{column_names_}
        , storage{storage_}
        , storage_snapshot{storage_snapshot_}
    {
    }

    String getName() const override { return "ReadFromStorageKafka"; }

private:
    Pipe makePipe() final
    {
        auto & kafka_storage = storage->as<StorageKafka &>();
        if (kafka_storage.shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "Table is detached");

        if (kafka_storage.mv_attached)
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageKafka with attached materialized views");

        ProfileEvents::increment(ProfileEvents::KafkaDirectReads);

        /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
        Pipes pipes;
        pipes.reserve(kafka_storage.num_consumers);
        auto modified_context = Context::createCopy(getContext());
        modified_context->applySettingsChanges(kafka_storage.settings_adjustments);

        // Claim as many consumers as requested, but don't block
        for (size_t i = 0; i < kafka_storage.num_consumers; ++i)
        {
            /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
            /// TODO: probably that leads to awful performance.
            /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
            pipes.emplace_back(std::make_shared<KafkaSource>(
                kafka_storage,
                storage_snapshot,
                modified_context,
                column_names,
                kafka_storage.log,
                1,
                kafka_storage.kafka_settings->kafka_commit_on_select));
        }

        LOG_DEBUG(kafka_storage.log, "Starting reading {} streams", pipes.size());
        return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

StorageKafka::StorageKafka(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<KafkaSettings> kafka_settings_,
    const String & collection_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , kafka_settings(std::move(kafka_settings_))
    , macros_info{.table_id = table_id_}
    , topics(StorageKafkaUtils::parseTopics(getContext()->getMacros()->expand(kafka_settings->kafka_topic_list.value, macros_info)))
    , brokers(getContext()->getMacros()->expand(kafka_settings->kafka_broker_list.value, macros_info))
    , group(getContext()->getMacros()->expand(kafka_settings->kafka_group_name.value, macros_info))
    , client_id(
          kafka_settings->kafka_client_id.value.empty()
              ? StorageKafkaUtils::getDefaultClientId(table_id_)
              : getContext()->getMacros()->expand(kafka_settings->kafka_client_id.value, macros_info))
    , format_name(getContext()->getMacros()->expand(kafka_settings->kafka_format.value))
    , max_rows_per_message(kafka_settings->kafka_max_rows_per_message.value)
    , schema_name(getContext()->getMacros()->expand(kafka_settings->kafka_schema.value, macros_info))
    , num_consumers(kafka_settings->kafka_num_consumers.value)
    , log(getLogger("StorageKafka (" + table_id_.table_name + ")"))
    , intermediate_commit(kafka_settings->kafka_commit_every_batch.value)
    , settings_adjustments(StorageKafkaUtils::createSettingsAdjustments(*kafka_settings, schema_name))
    , thread_per_consumer(kafka_settings->kafka_thread_per_consumer.value)
    , collection_name(collection_name_)
{
    kafka_settings->sanityCheck();

    if (kafka_settings->kafka_handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        kafka_settings->input_format_allow_errors_num = 0;
        kafka_settings->input_format_allow_errors_ratio = 0;
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(StorageKafkaUtils::createVirtuals(kafka_settings->kafka_handle_error_mode));

    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i]{ threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }

    consumers.resize(num_consumers);
    for (size_t i = 0; i < num_consumers; ++i)
        consumers[i] = createKafkaConsumer(i);

    cleanup_thread = std::make_unique<ThreadFromGlobalPool>([this]()
    {
        const auto & table = getStorageID().getTableName();
        const auto & thread_name = std::string("KfkCln:") + table;
        setThreadName(thread_name.c_str(), /*truncate=*/ true);
        cleanConsumers();
    });
}

StorageKafka::~StorageKafka() = default;

void StorageKafka::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    query_plan.addStep(std::make_unique<ReadFromStorageKafka>(
        column_names, shared_from_this(), storage_snapshot, query_info, std::move(query_context)));
}


SinkToStoragePtr StorageKafka::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->applySettingsChanges(settings_adjustments);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaWrites};
    ProfileEvents::increment(ProfileEvents::KafkaWrites);

    if (topics.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't write to Kafka table with multiple topics!");

    cppkafka::Configuration conf = getProducerConfiguration();

    const Settings & settings = getContext()->getSettingsRef();
    size_t poll_timeout = settings[Setting::stream_poll_timeout_ms].totalMilliseconds();
    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), topics[0], std::chrono::milliseconds(poll_timeout), shutdown_called, header);

    LOG_TRACE(log, "Kafka producer created");

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(
        header, getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}


void StorageKafka::startup()
{
    // Start the reader thread
    for (auto & task : tasks)
    {
        task->holder->activateAndSchedule();
    }
}


void StorageKafka::shutdown(bool)
{
    shutdown_called = true;
    cleanup_cv.notify_one();

    {
        LOG_TRACE(log, "Waiting for consumers cleanup thread");
        Stopwatch watch;
        if (cleanup_thread)
        {
            cleanup_thread->join();
            cleanup_thread.reset();
        }
        LOG_TRACE(log, "Consumers cleanup thread finished in {} ms.", watch.elapsedMilliseconds());
    }

    {
        LOG_TRACE(log, "Waiting for streaming jobs");
        Stopwatch watch;
        for (auto & task : tasks)
        {
            // Interrupt streaming thread
            task->stream_cancelled = true;

            LOG_TEST(log, "Waiting for cleanup of a task");
            task->holder->deactivate();
        }
        LOG_TRACE(log, "Streaming jobs finished in {} ms.", watch.elapsedMilliseconds());
    }

    {
        std::lock_guard lock(mutex);
        LOG_TRACE(log, "Closing {} consumers", consumers.size());
        Stopwatch watch;
        consumers.clear();
        LOG_TRACE(log, "Consumers closed. Took {} ms.", watch.elapsedMilliseconds());
    }

    {
        LOG_TRACE(log, "Waiting for final cleanup");
        Stopwatch watch;
        rd_kafka_wait_destroyed(KAFKA_CLEANUP_TIMEOUT_MS);
        LOG_TRACE(log, "Final cleanup finished in {} ms (timeout {} ms).", watch.elapsedMilliseconds(), KAFKA_CLEANUP_TIMEOUT_MS);
    }
}


void StorageKafka::pushConsumer(KafkaConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumer->notInUse();
    cv.notify_one();
    CurrentMetrics::sub(CurrentMetrics::KafkaConsumersInUse, 1);
}


KafkaConsumerPtr StorageKafka::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}


KafkaConsumerPtr StorageKafka::popConsumer(std::chrono::milliseconds timeout)
{
    std::unique_lock lock(mutex);

    KafkaConsumerPtr ret_consumer_ptr;
    std::optional<size_t> closed_consumer_index;
    for (size_t i = 0; i < consumers.size(); ++i)
    {
        auto & consumer_ptr = consumers[i];

        if (consumer_ptr->isInUse())
            continue;

        if (consumer_ptr->hasConsumer())
        {
            ret_consumer_ptr = consumer_ptr;
            break;
        }

        if (!closed_consumer_index.has_value() && !consumer_ptr->hasConsumer())
        {
            closed_consumer_index = i;
        }
    }

    /// 1. There is consumer available - return it.
    if (ret_consumer_ptr)
    {
        /// Noop
    }
    /// 2. There is no consumer, but we can create a new one.
    else if (!ret_consumer_ptr && closed_consumer_index.has_value())
    {
        ret_consumer_ptr = consumers[*closed_consumer_index];

        cppkafka::Configuration consumer_config = getConsumerConfiguration(*closed_consumer_index);
        /// It should be OK to create consumer under lock, since it should be fast (without subscribing).
        ret_consumer_ptr->createConsumer(consumer_config);
        LOG_TRACE(log, "Created #{} consumer", *closed_consumer_index);
    }
    /// 3. There is no free consumer and num_consumers already created, waiting @timeout.
    else
    {
        cv.wait_for(lock, timeout, [&]()
        {
            /// Note we are waiting only opened, free, consumers, since consumer cannot be closed right now
            auto it = std::find_if(consumers.begin(), consumers.end(), [](const auto & ptr)
            {
                return !ptr->isInUse() && ptr->hasConsumer();
            });
            if (it != consumers.end())
            {
                ret_consumer_ptr = *it;
                return true;
            }
            return false;
        });
    }

    if (ret_consumer_ptr)
    {
        CurrentMetrics::add(CurrentMetrics::KafkaConsumersInUse, 1);
        ret_consumer_ptr->inUse();
    }
    return ret_consumer_ptr;
}


KafkaConsumerPtr StorageKafka::createKafkaConsumer(size_t consumer_number)
{
    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    auto & stream_cancelled = thread_per_consumer ? tasks[consumer_number]->stream_cancelled : tasks.back()->stream_cancelled;

    KafkaConsumerPtr kafka_consumer_ptr = std::make_shared<KafkaConsumer>(
        log,
        getPollMaxBatchSize(),
        getPollTimeoutMillisecond(),
        intermediate_commit,
        stream_cancelled,
        topics);
    return kafka_consumer_ptr;
}
cppkafka::Configuration StorageKafka::getConsumerConfiguration(size_t consumer_number)
{
    KafkaConfigLoader::ConsumerConfigParams params{
        {getContext()->getConfigRef(), collection_name, topics, log},
        brokers,
        group,
        num_consumers > 1,
        consumer_number,
        client_id,
        getMaxBlockSize()};
    return KafkaConfigLoader::getConsumerConfiguration(*this, params);
}

cppkafka::Configuration StorageKafka::getProducerConfiguration()
{
    KafkaConfigLoader::ProducerConfigParams params{
        {getContext()->getConfigRef(), collection_name, topics, log},
        brokers,
        client_id};
    return KafkaConfigLoader::getProducerConfiguration(*this, params);
}

void StorageKafka::cleanConsumers()
{
    UInt64 ttl_usec = kafka_settings->kafka_consumers_pool_ttl_ms * 1'000;

    std::unique_lock lock(mutex);
    std::chrono::milliseconds timeout(KAFKA_RESCHEDULE_MS);
    while (!cleanup_cv.wait_for(lock, timeout, [this]() { return shutdown_called == true; }))
    {
        /// Copy consumers for closing to a new vector to close them without a lock
        std::vector<ConsumerPtr> consumers_to_close;

        UInt64 now_usec = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        {
            for (size_t i = 0; i < consumers.size(); ++i)
            {
                auto & consumer_ptr = consumers[i];

                UInt64 consumer_last_used_usec = consumer_ptr->getLastUsedUsec();
                chassert(consumer_last_used_usec <= now_usec);

                if (!consumer_ptr->hasConsumer())
                    continue;
                if (consumer_ptr->isInUse())
                    continue;

                if (now_usec - consumer_last_used_usec > ttl_usec)
                {
                    LOG_TRACE(log, "Closing #{} consumer (id: {})", i, consumer_ptr->getMemberId());
                    consumers_to_close.push_back(consumer_ptr->moveConsumer());
                }
            }
        }

        if (!consumers_to_close.empty())
        {
            lock.unlock();

            Stopwatch watch;
            size_t closed = consumers_to_close.size();
            consumers_to_close.clear();
            LOG_TRACE(log, "{} consumers had been closed (due to {} usec timeout). Took {} ms.",
                closed, ttl_usec, watch.elapsedMilliseconds());

            lock.lock();
        }

        ttl_usec = kafka_settings->kafka_consumers_pool_ttl_ms * 1'000;
    }

    LOG_TRACE(log, "Consumers cleanup thread finished");
}

size_t StorageKafka::getMaxBlockSize() const
{
    return kafka_settings->kafka_max_block_size.changed ? kafka_settings->kafka_max_block_size.value
                                                        : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}

size_t StorageKafka::getPollMaxBatchSize() const
{
    size_t batch_size = kafka_settings->kafka_poll_max_batch_size.changed ? kafka_settings->kafka_poll_max_batch_size.value
                                                                          : getContext()->getSettingsRef()[Setting::max_block_size].value;

    return std::min(batch_size,getMaxBlockSize());
}

size_t StorageKafka::getPollTimeoutMillisecond() const
{
    return kafka_settings->kafka_poll_timeout_ms.changed ? kafka_settings->kafka_poll_timeout_ms.totalMilliseconds()
                                                         : getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
}

void StorageKafka::threadFunc(size_t idx)
{
    assert(idx < tasks.size());
    auto task = tasks[idx];
    std::string exception_str;

    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
        if (num_views)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!StorageKafkaUtils::checkDependencies(table_id, getContext()))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

                // Exit the loop & reschedule if some stream stalled
                auto some_stream_is_stalled = streamToViews();
                if (some_stream_is_stalled)
                {
                    LOG_TRACE(log, "Stream(s) stalled. Reschedule.");
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > KAFKA_MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    break;
                }
            }
        }
    }
    catch (...)
    {
        /// do bare minimum in catch block
        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
        exception_str = getCurrentExceptionMessage(true /* with_stacktrace */);
    }

    if (!exception_str.empty())
    {
        LOG_ERROR(log, "{} {}", __PRETTY_FUNCTION__, exception_str);

        auto safe_consumers = getSafeConsumers();
        for (auto const & consumer_ptr : safe_consumers.consumers)
        {
            /// propagate materialized view exception to all consumers
            consumer_ptr->setExceptionInfo(exception_str, false /* no stacktrace, reuse passed one */);
        }
    }

    mv_attached.store(false);

    // Wait for attached views
    if (!task->stream_cancelled)
        task->holder->scheduleAfter(KAFKA_RESCHEDULE_MS);
}


bool StorageKafka::streamToViews()
{
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaBackgroundReads};
    ProfileEvents::increment(ProfileEvents::KafkaBackgroundReads);

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto kafka_context = Context::createCopy(getContext());
    kafka_context->makeQueryContext();
    kafka_context->applySettingsChanges(settings_adjustments);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        kafka_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<KafkaSource>> sources;
    Pipes pipes;

    auto stream_count = thread_per_consumer ? 1 : num_consumers;
    sources.reserve(stream_count);
    pipes.reserve(stream_count);
    for (size_t i = 0; i < stream_count; ++i)
    {
        auto source = std::make_shared<KafkaSource>(*this, storage_snapshot, kafka_context, block_io.pipeline.getHeader().getNames(), log, block_size, false);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        Poco::Timespan max_execution_time = kafka_settings->kafka_flush_interval_ms.changed
            ? kafka_settings->kafka_flush_interval_ms
            : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

        source->setTimeLimit(max_execution_time);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    // We can't cancel during copyData, as it's not aware of commits and other kafka-related stuff.
    // It will be cancelled on underlying layer (kafka buffer)

    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(std::move(pipe));

        // we need to read all consumers in parallel (sequential read may lead to situation
        // when some of consumers are not used, and will break some Kafka consumer invariants)
        block_io.pipeline.setNumThreads(stream_count);
        block_io.pipeline.setConcurrencyControl(kafka_context->getSettingsRef()[Setting::use_concurrency_control]);

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    bool some_stream_is_stalled = false;
    for (auto & source : sources)
    {
        some_stream_is_stalled = some_stream_is_stalled || source->isStalled();
        source->commit();
    }

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.",
        formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return some_stream_is_stalled;
}
}
