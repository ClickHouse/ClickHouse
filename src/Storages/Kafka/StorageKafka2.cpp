#include <Storages/Kafka/StorageKafka2.h>

#include <Columns/IColumn.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/DeadLetterQueue.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Sources/BlocksListSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnDefault.h>
#include <Storages/Kafka/KafkaConfigLoader.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <base/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/UniqueLock.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/config_version.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <librdkafka/rdkafka.h>
#include <pcg-random/pcg_random.hpp>

namespace CurrentMetrics
{
extern const Metric KafkaConsumersInUse;
extern const Metric KafkaBackgroundReads;
extern const Metric KafkaWrites;
}

namespace ProfileEvents
{
extern const Event KafkaBackgroundReads;
extern const Event KafkaMessagesRead;
extern const Event KafkaMessagesFailed;
extern const Event KafkaRowsRead;
extern const Event KafkaWrites;
extern const Event KafkaMVNotReady;
}


namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
}

namespace KafkaSetting
{
    extern const KafkaSettingsUInt64 input_format_allow_errors_num;
    extern const KafkaSettingsFloat input_format_allow_errors_ratio;
    extern const KafkaSettingsString kafka_broker_list;
    extern const KafkaSettingsString kafka_client_id;
    extern const KafkaSettingsMilliseconds kafka_flush_interval_ms;
    extern const KafkaSettingsMilliseconds kafka_consumer_reschedule_ms;
    extern const KafkaSettingsString kafka_format;
    extern const KafkaSettingsString kafka_group_name;
    extern const KafkaSettingsStreamingHandleErrorMode kafka_handle_error_mode;
    extern const KafkaSettingsString kafka_keeper_path;
    extern const KafkaSettingsUInt64 kafka_max_block_size;
    extern const KafkaSettingsUInt64 kafka_max_rows_per_message;
    extern const KafkaSettingsUInt64 kafka_num_consumers;
    extern const KafkaSettingsUInt64 kafka_poll_max_batch_size;
    extern const KafkaSettingsMilliseconds kafka_poll_timeout_ms;
    extern const KafkaSettingsString kafka_replica_name;
    extern const KafkaSettingsString kafka_schema;
    extern const KafkaSettingsUInt64 kafka_schema_registry_skip_bytes;
    extern const KafkaSettingsBool kafka_thread_per_consumer;
    extern const KafkaSettingsString kafka_topic_list;
}

namespace fs = std::filesystem;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int REPLICA_ALREADY_EXISTS;
extern const int TABLE_IS_DROPPED;
extern const int NO_ZOOKEEPER;
extern const int REPLICA_IS_ALREADY_ACTIVE;
}

namespace
{
constexpr auto MAX_FAILED_POLL_ATTEMPTS = 10;
}

StorageKafka2::StorageKafka2(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<KafkaSettings> kafka_settings_,
    const String & collection_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , keeper(getContext()->getZooKeeper())
    , keeper_path((*kafka_settings_)[KafkaSetting::kafka_keeper_path].value)
    , fs_keeper_path(keeper_path)
    , replica_path(keeper_path + "/replicas/" + (*kafka_settings_)[KafkaSetting::kafka_replica_name].value)
    , kafka_settings(std::move(kafka_settings_))
    , macros_info{.table_id = table_id_}
    , topics(StorageKafkaUtils::parseTopics(getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_topic_list].value, macros_info)))
    , brokers(getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_broker_list].value, macros_info))
    , group(getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_group_name].value, macros_info))
    , client_id(
          (*kafka_settings)[KafkaSetting::kafka_client_id].value.empty()
              ? StorageKafkaUtils::getDefaultClientId(table_id_)
              : getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_client_id].value, macros_info))
    , format_name(getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_format].value))
    , max_rows_per_message((*kafka_settings)[KafkaSetting::kafka_max_rows_per_message].value)
    , schema_name(getContext()->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_schema].value, macros_info))
    , num_consumers((*kafka_settings)[KafkaSetting::kafka_num_consumers].value)
    , log(getLogger("StorageKafka2 (" + table_id_.getNameForLogs() + ")"))
    , settings_adjustments(StorageKafkaUtils::createSettingsAdjustments(*kafka_settings, schema_name))
    , thread_per_consumer((*kafka_settings)[KafkaSetting::kafka_thread_per_consumer].value)
    , collection_name(collection_name_)
    , active_node_identifier(toString(ServerUUID::get()))
{
    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::StorageKafka2");
    kafka_settings->sanityCheck(getContext());
    if ((*kafka_settings)[KafkaSetting::kafka_num_consumers] > 1 && !thread_per_consumer)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "With multiple consumers, it is required to use `kafka_thread_per_consumer` setting");

    if (auto mode = getHandleKafkaErrorMode();
        mode == StreamingHandleErrorMode::STREAM || mode == StreamingHandleErrorMode::DEAD_LETTER_QUEUE)
    {
        (*kafka_settings)[KafkaSetting::input_format_allow_errors_num] = 0;
        (*kafka_settings)[KafkaSetting::input_format_allow_errors_ratio] = 0;
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(StorageKafkaUtils::createVirtuals(getHandleKafkaErrorMode()));

    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), log->name(), [this, i] { threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }

    const auto first_replica = createTableIfNotExists();

    if (!first_replica)
        createReplica();

    activating_task = getContext()->getSchedulePool().createTask(getStorageID(), log->name() + " (activating task)", [this]() { activateAndReschedule(); });
    activating_task->deactivate();
}

StorageKafka2::~StorageKafka2()
{
    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::~StorageKafka2");
    replica_is_active_node.reset();
}

void StorageKafka2::partialShutdown()
{
    // This is called in a background task within a catch block, thus this function shouldn't throw
    LOG_TRACE(log, "Cancelling streams");
    for (auto & task : tasks)
    {
        task->stream_cancelled = true;
    }

    LOG_TRACE(log, "Waiting for cleanup");
    for (auto & task : tasks)
    {
        task->holder->deactivate();
    }
    is_active = false;
    /// Reset the active node holder while the old ZooKeeper session is still alive (even if expired).
    /// EphemeralNodeHolder stores a raw ZooKeeper reference, so resetting it here prevents a
    /// use-after-free: setZooKeeper() called afterwards may free the old session, and the holder's
    /// destructor would then access a dangling reference when checking zookeeper.expired().
    replica_is_active_node = nullptr;
}

bool StorageKafka2::activate()
{
    LOG_TEST(log, "Activate task");
    if (is_active && !getZooKeeper()->expired())
    {
        LOG_TEST(log, "No need to activate");
        return true;
    }

    if (!is_active)
    {
        LOG_WARNING(log, "Table was not active. Will try to activate it");
    }
    else if (getZooKeeper()->expired())
    {
        LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session");
        partialShutdown();
    }
    else
    {
        UNREACHABLE();
    }

    try
    {
        setZooKeeper();
    }
    catch (const Coordination::Exception &)
    {
        /// The exception when you try to zookeeper_init usually happens if DNS does not work or the connection with ZK fails
        tryLogCurrentException(log, "Failed to establish a new ZK connection. Will try again");
        assert(!is_active);
        return false;
    }

    if (shutdown_called)
        return false;

    auto activate_in_keeper = [this]()
    {
        try
        {
            auto zookeeper = getZooKeeper();

            String is_active_path = fs::path(replica_path) / "is_active";
            zookeeper->deleteEphemeralNodeIfContentMatches(is_active_path, active_node_identifier);

            try
            {
                /// Simultaneously declare that this replica is active, and update the host.
                zookeeper->create(is_active_path, active_node_identifier, zkutil::CreateMode::Ephemeral);
            }
            catch (const Coordination::Exception & e)
            {
                if (e.code == Coordination::Error::ZNODEEXISTS)
                    throw Exception(
                        ErrorCodes::REPLICA_IS_ALREADY_ACTIVE,
                        "Replica {} appears to be already active. If you're sure it's not, "
                        "try again in a minute or remove znode {}/is_active manually",
                        replica_path,
                        replica_path);

                throw;
            }
            replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *zookeeper);

            return true;
        }
        catch (const Coordination::Exception & e)
        {
            replica_is_active_node = nullptr;
            LOG_ERROR(log, "Couldn't start replica: {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;

        }
        catch (const Exception & e)
        {
            replica_is_active_node = nullptr;
            if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
                throw;

            LOG_ERROR(log, "Couldn't start replica: {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;
        }
    };

    if (!activate_in_keeper())
    {
        assert(!is_active);
        return false;
    }

    is_active = true;

    // Start the reader threads
    for (auto & task : tasks)
    {
        task->stream_cancelled = false;
        task->holder->activateAndSchedule();
    }

    LOG_DEBUG(log, "Table activated successfully");
    return true;
}

void StorageKafka2::activateAndReschedule()
{
    if (shutdown_called)
        return;

    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::activateAndReschedule");
    /// It would be ideal to introduce a setting for this
    constexpr static size_t check_period_ms = 60000;
    /// In case of any exceptions we want to rerun the this task as fast as possible but we also don't want to keep
    /// retrying immediately in a close loop (as fast as tasks can be processed), so we'll retry in between 100 and
    /// 10000 ms
    const size_t backoff_ms = 100 * ((consecutive_activate_failures + 1) * (consecutive_activate_failures + 2)) / 2;
    const size_t next_failure_retry_ms = std::min(size_t{10000}, backoff_ms);

    try
    {
        bool replica_is_active = activate();
        if (replica_is_active)
        {
            consecutive_activate_failures = 0;
            activating_task->scheduleAfter(check_period_ms);
        }
        else
        {
            consecutive_activate_failures++;
            activating_task->scheduleAfter(next_failure_retry_ms);
        }
    }
    catch (...)
    {
        consecutive_activate_failures++;
        activating_task->scheduleAfter(next_failure_retry_ms);

        /// We couldn't activate table let's set it into readonly mode if necessary
        /// We do this after scheduling the task in case it throws
        partialShutdown();
        tryLogCurrentException(log, "Failed to restart the table. Will try again");
    }
}

void StorageKafka2::assertActive() const
{
    // TODO(antaljanosbenjamin): change LOGICAL_ERROR to something sensible
    if (!is_active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table is not active (replica path: {})", replica_path);
}


Pipe StorageKafka2::read(
    const Names & /*column_names */,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & /* query_info */,
    ContextPtr /* local_context */,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct read from the new Kafka storage is not implemented");
}

StreamingHandleErrorMode StorageKafka2::getHandleKafkaErrorMode() const
{
    return (*kafka_settings)[KafkaSetting::kafka_handle_error_mode];
}


SinkToStoragePtr
StorageKafka2::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
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
    auto header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), topics[0], std::chrono::milliseconds(poll_timeout), shutdown_called, header);

    LOG_TRACE(log, "Kafka producer created");

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(std::make_shared<const Block>(std::move(header)), getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}

void StorageKafka2::startup()
{
    const auto replica_name = (*kafka_settings)[KafkaSetting::kafka_replica_name].value;
    {
        std::lock_guard lock(consumers_mutex);
        for (size_t i = 0; i < num_consumers; ++i)
        {
            try
            {
                consumers.push_back(
                    std::make_shared<KeeperHandlingConsumer>(createKafkaConsumer(i), getZooKeeper(), fs_keeper_path, replica_name, i, log));
                ++num_created_consumers;
            }
            catch (const cppkafka::Exception &)
            {
                tryLogCurrentException(log);
            }
        }
    }
    activating_task->activateAndSchedule();
}


void StorageKafka2::shutdown(bool)
{
    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::shutdown");
    shutdown_called = true;
    activating_task->deactivate();
    partialShutdown();
    LOG_TRACE(log, "Closing consumers");
    cleanConsumers();
    LOG_TRACE(log, "Consumers closed");
}

void StorageKafka2::drop()
{
    dropReplica();
}

KafkaConsumer2Ptr StorageKafka2::createKafkaConsumer(size_t consumer_number)
{
    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    chassert((thread_per_consumer || num_consumers == 1) && "StorageKafka2 cannot handle multiple consumers on a single thread");
    auto & stream_cancelled = tasks[consumer_number]->stream_cancelled;
    return std::make_shared<KafkaConsumer2>(log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), stream_cancelled, topics, getSchemaRegistrySkipBytes());
}

cppkafka::Configuration StorageKafka2::getConsumerConfiguration(size_t consumer_number, IKafkaExceptionInfoSinkPtr exception_sink)
{
    KafkaConfigLoader::ConsumerConfigParams params{
        {getContext()->getConfigRef(), collection_name, topics, log},
        brokers,
        group,
        num_consumers > 1,
        consumer_number,
        client_id,
        getMaxBlockSize()};
    auto kafka_config = KafkaConfigLoader::getConsumerConfiguration(*this, params, std::move(exception_sink));
    // It is disabled, because in case of no materialized views are attached, it can cause live memory leak. To enable it, a similar cleanup mechanism must be introduced as for StorageKafka.
    kafka_config.set("statistics.interval.ms", "0");
    // Making more frequent updates, now 1 min
    kafka_config.set("topic.metadata.refresh.interval.ms", "60000");
    return kafka_config;
}

cppkafka::Configuration StorageKafka2::getProducerConfiguration()
{
    KafkaConfigLoader::ProducerConfigParams params{
        {getContext()->getConfigRef(), collection_name, topics, log},
        brokers,
        client_id};
    return KafkaConfigLoader::getProducerConfiguration(*this, params);
}

size_t StorageKafka2::getMaxBlockSize() const
{
    return (*kafka_settings)[KafkaSetting::kafka_max_block_size].changed ? (*kafka_settings)[KafkaSetting::kafka_max_block_size].value
                                                        : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}

size_t StorageKafka2::getPollMaxBatchSize() const
{
    size_t batch_size = (*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].changed ? (*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].value
                                                                          : getContext()->getSettingsRef()[Setting::max_block_size].value;

    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageKafka2::getPollTimeoutMillisecond() const
{
    return (*kafka_settings)[KafkaSetting::kafka_poll_timeout_ms].changed ? (*kafka_settings)[KafkaSetting::kafka_poll_timeout_ms].totalMilliseconds()
                                                         : getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
}

size_t StorageKafka2::getSchemaRegistrySkipBytes() const
{
    return (*kafka_settings)[KafkaSetting::kafka_schema_registry_skip_bytes].value;
}

bool StorageKafka2::createTableIfNotExists()
{
    // Heavily based on StorageReplicatedMergeTree::createTableIfNotExists
    const auto replicas_path = fs_keeper_path / "replicas";

    for (auto i = 0; i < 1000; ++i)
    {
        if (keeper->exists(replicas_path))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", keeper_path);
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (keeper->exists(fs_keeper_path / "dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {}", keeper_path);
            String drop_lock_path = fs_keeper_path / "dropped" / "lock";
            Coordination::Error code = keeper->tryCreate(drop_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "The leftovers from table {} were removed by another replica", keeper_path);
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception::fromPath(code, drop_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *keeper);
                if (!removeTableNodesFromZooKeeper(keeper, metadata_drop_lock))
                {
                    /// Someone is recursively removing table right now, we cannot create new table until old one is removed
                    continue;
                }
            }
        }

        keeper->createAncestors(keeper_path);
        Coordination::Requests ops;

        ops.emplace_back(zkutil::makeCreateRequest(keeper_path, "", zkutil::CreateMode::Persistent));

        const auto topics_path = fs_keeper_path / "topics";
        ops.emplace_back(zkutil::makeCreateRequest(topics_path, "", zkutil::CreateMode::Persistent));

        for (const auto & topic : topics)
        {
            LOG_DEBUG(log, "Creating path in keeper for topic {}", topic);

            const auto topic_path = topics_path / topic;
            ops.emplace_back(zkutil::makeCreateRequest(topic_path, "", zkutil::CreateMode::Persistent));

            const auto partitions_path = topic_path / "partitions";
            ops.emplace_back(zkutil::makeCreateRequest(partitions_path, "", zkutil::CreateMode::Persistent));
        }

        // Create the path for topic partition locks
        const auto topic_partition_locks_path = fs_keeper_path / "topic_partition_locks";
        ops.emplace_back(zkutil::makeCreateRequest(topic_partition_locks_path, "", zkutil::CreateMode::Persistent));

        // Create the first replica
        ops.emplace_back(zkutil::makeCreateRequest(replicas_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));


        Coordination::Responses responses;
        const auto code = keeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another replica at the same moment, will retry", keeper_path);
            continue;
        }
        if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        LOG_INFO(log, "Table {} created successfully ", keeper_path);

        return true;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper_path or because of logical error");
}


bool StorageKafka2::removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr keeper_to_use, const zkutil::EphemeralNodeHolder::Ptr & drop_lock)
{
    bool completely_removed = false;

    Strings children;
    if (const auto code = keeper_to_use->tryGetChildren(keeper_path, children); code == Coordination::Error::ZNONODE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal. It's a bug");

    for (const auto & child : children)
        if (child != "dropped")
            keeper_to_use->tryRemoveRecursive(fs_keeper_path / child);

    Coordination::Requests ops;
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(fs_keeper_path / "dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(fs_keeper_path, -1));
    const auto code = keeper_to_use->tryMulti(ops, responses, /* check_session_valid */ true);

    if (code == Coordination::Error::ZNONODE)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");
    }
    if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_ERROR(
            log,
            "Table was not completely removed from Keeper, {} still exists and may contain some garbage,"
            "but someone is removing it right now.",
            keeper_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        drop_lock->setAlreadyRemoved();
        completely_removed = true;
        LOG_INFO(log, "Table {} was successfully removed from ZooKeeper", keeper_path);
    }

    return completely_removed;
}

void StorageKafka2::createReplica()
{
    LOG_INFO(log, "Creating replica {}", replica_path);
    // TODO: This can cause issues if a new table is created with the same path. To make this work, we should store some
    // metadata about the table to be able to identify that the same table is created, not a new one.
    const auto code = keeper->tryCreate(replica_path, "", zkutil::CreateMode::Persistent);

    switch (code)
    {
        case Coordination::Error::ZNODEEXISTS:
            LOG_INFO(log, "Replica {} already exists, will try to use it", replica_path);
            break;
        case Coordination::Error::ZOK:
            LOG_INFO(log, "Replica {} created", replica_path);
            break;
        case Coordination::Error::ZNONODE:
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} was suddenly removed", keeper_path);
        default:
            throw Coordination::Exception::fromPath(code, replica_path);
    }
}


void StorageKafka2::dropReplica()
{
    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::dropReplica");
    LOG_INFO(log, "Trying to drop replica {}", replica_path);
    auto my_keeper = getZooKeeperIfTableShutDown();

    LOG_INFO(log, "Removing replica {}", replica_path);

    if (!my_keeper->exists(replica_path))
    {
        LOG_INFO(log, "Removing replica {} does not exist", replica_path);
        return;
    }

    my_keeper->tryRemoveChildrenRecursive(replica_path);

    if (my_keeper->tryRemove(replica_path) != Coordination::Error::ZOK)
        LOG_ERROR(log, "Replica was not completely removed from Keeper, {} still exists and may contain some garbage.", replica_path);

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (Coordination::Error::ZOK != my_keeper->tryGetChildren(keeper_path + "/replicas", replicas) || !replicas.empty())
        return;

    LOG_INFO(log, "{} is the last replica, will remove table", replica_path);

    /** At this moment, another replica can be created and we cannot remove the table. Try to remove /replicas node
      * first. If we successfully removed it, it guarantees that we are the only replica that proceed to remove the
      * table and no new replicas can be created after that moment (it requires the existence of /replicas node). and
      * table cannot be recreated with new /replicas node on another servers while we are removing data, because table
      * creation is executed in single transaction that will conflict with remaining nodes.
      */

    /// Node /dropped works like a lock that protects from concurrent removal of old table and creation of new table.
    /// But recursive removal may fail in the middle of operation leaving some garbage in zookeeper_path, so we remove
    /// it on table creation if there is /dropped node. Creating thread may remove /dropped node created by removing
    /// thread, and it causes race condition if removing thread is not finished yet. To avoid this we also create
    /// ephemeral child before starting recursive removal. (The existence of child node does not allow to remove parent
    /// node).
    Coordination::Requests ops;
    Coordination::Responses responses;
    String drop_lock_path = fs_keeper_path / "dropped" / "lock";
    ops.emplace_back(zkutil::makeRemoveRequest(fs_keeper_path / "replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(fs_keeper_path / "dropped", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
    Coordination::Error code = my_keeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_WARNING(log, "Table {} is already started to be removing by another replica right now", replica_path);
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_WARNING(log, "Another replica was suddenly created, will keep the table {}", replica_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        auto drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *my_keeper);
        LOG_INFO(log, "Removing table {} (this might take several minutes)", keeper_path);
        removeTableNodesFromZooKeeper(my_keeper, drop_lock);
    }
}

std::optional<StorageKafka2::BlocksAndGuard> StorageKafka2::pollConsumer(
    KeeperHandlingConsumer & consumer,
    const Stopwatch & watch,
    const ContextPtr & modified_context)
{
    LOG_TEST(log, "Polling consumer");
    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    Block non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized());
    auto virtual_header = getVirtualsHeader();

    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        getFormatName(),
        empty_buf,
        non_virtual_header,
        modified_context,
        getMaxBlockSize(),
        std::nullopt,
        FormatParserSharedResources::singleThreaded(modified_context->getSettingsRef()));

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;
    bool is_dead_letter = false;

    // Dirty hack to "pass" MessageInfo to the on_error lambda by reference. current_msg_info is captured in both
    // `on_error` and `msg_sink` lambdas. It is assigned in `msg_sink` in order to pass the necessary information to
    // `on_error` in case of an exception happens. Doing the same through a member variable would be worse, because then
    // we would need to think about multiple threads.
    const KeeperHandlingConsumer::MessageInfo * current_msg_info = nullptr;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        switch (getHandleKafkaErrorMode())
        {
            case StreamingHandleErrorMode::STREAM:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                {
                    // We could already push some rows to result_columns before exception, we need to fix it.
                    result_columns[i]->rollback(*checkpoints[i]);

                    // all data columns will get default value in case of error
                    result_columns[i]->insertDefault();
                }
                return 1;
            }
            case StreamingHandleErrorMode::DEAD_LETTER_QUEUE:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                {
                    // We could already push some rows to result_columns before exception, we need to fix it.
                    result_columns[i]->rollback(*checkpoints[i]);
                }

                is_dead_letter = true;
                return 0;
            }
            case StreamingHandleErrorMode::DEFAULT:
            {
                e.addMessage(
                    "while parsing Kafka message (topic: {}, partition: {}, offset: {})'",
                    current_msg_info->currentTopic(),
                    current_msg_info->currentPartition(),
                    current_msg_info->currentOffset());
                throw std::move(e);
            }
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));


    Poco::Timespan max_execution_time = (*kafka_settings)[KafkaSetting::kafka_flush_interval_ms].changed
        ? (*kafka_settings)[KafkaSetting::kafka_flush_interval_ms]
        : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

    const auto check_time_limit = [&max_execution_time, &watch]()
    {
        if (max_execution_time != 0)
        {
            auto elapsed_ns = watch.elapsed();

            if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
                return false;
        }

        return true;
    };

    KeeperHandlingConsumer::MessageSinkFunction msg_sink = [&](ReadBufferPtr buf, const KeeperHandlingConsumer::MessageInfo & msg_info, bool has_more_polled_messages, bool stalled) mutable
    {
        size_t new_rows = 0;
        exception_message.reset();

        is_dead_letter = false;
        if (buf)
        {
            current_msg_info = &msg_info;
            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);
            new_rows = executor.execute(*buf);
        }

        if (new_rows || is_dead_letter)
        {
            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

            const auto & header_list = msg_info.currentHeaderList();

            Array headers_names;
            Array headers_values;

            if (!header_list.empty())
            {
                headers_names.reserve(header_list.size());
                headers_values.reserve(header_list.size());
                for (const auto & header : header_list)
                {
                    headers_names.emplace_back(header.get_name());
                    headers_values.emplace_back(static_cast<std::string>(header.get_value()));
                }
            }

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(msg_info.currentTopic());
                virtual_columns[1]->insert(msg_info.currentKey());
                virtual_columns[2]->insert(msg_info.currentOffset());
                virtual_columns[3]->insert(msg_info.currentPartition());


                auto timestamp_raw = msg_info.currentTimestamp();
                if (timestamp_raw)
                {
                    auto ts = timestamp_raw->get_timestamp();
                    virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(ts).count());
                    virtual_columns[5]->insert(
                        DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(), 3));
                }
                else
                {
                    virtual_columns[4]->insertDefault();
                    virtual_columns[5]->insertDefault();
                }
                virtual_columns[6]->insert(headers_names);
                virtual_columns[7]->insert(headers_values);

                if (getHandleKafkaErrorMode() == StreamingHandleErrorMode::STREAM)
                {
                    if (exception_message)
                    {
                        virtual_columns[8]->insert(msg_info.currentPayload());
                        virtual_columns[9]->insert(*exception_message);
                    }
                    else
                    {
                        virtual_columns[8]->insertDefault();
                        virtual_columns[9]->insertDefault();
                    }
                }
            }

            if (is_dead_letter)
            {
                assert(exception_message);
                const auto time_now = std::chrono::system_clock::now();
                auto storage_id = getStorageID();

                auto dead_letter_queue = getContext()->getDeadLetterQueue();
                if (!dead_letter_queue)
                    LOG_WARNING(log, "Table system.dead_letter_queue is not configured, skipping message");
                else
                    dead_letter_queue->add(
                        DeadLetterQueueElement{
                            .table_engine = DeadLetterQueueElement::StreamType::Kafka,
                            .event_time = timeInSeconds(time_now),
                            .event_time_microseconds = timeInMicroseconds(time_now),
                            .database = storage_id.database_name,
                            .table = storage_id.table_name,
                            .raw_message = msg_info.currentPayload(),
                            .error = exception_message.value(),
                            .details = DeadLetterQueueElement::KafkaDetails{
                                .topic_name = msg_info.currentTopic(),
                                .partition = msg_info.currentPartition(),
                                .offset = msg_info.currentPartition(),
                                .key = msg_info.currentKey()}});
            }

            total_rows = total_rows + new_rows;
        }
        else if (stalled)
        {
            ++failed_poll_attempts;
        }
        else
        {
            // We came here in case of tombstone (or sometimes zero-length) messages, and it is not something abnormal
            // TODO: it seems like in case of StreamingHandleErrorMode::STREAM or DEAD_LETTER_QUEUE
            //  we may need to process those differently
            //  currently we just skip them with note in logs.
            LOG_DEBUG(
                log,
                "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
                msg_info.currentTopic(),
                msg_info.currentPartition(),
                msg_info.currentOffset());
        }

        if (!has_more_polled_messages
            && (total_rows >= getMaxBlockSize() || !check_time_limit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            LOG_TRACE(
                log,
                "Stopped collecting message for current batch. There are {} failed polled attempts, {} total rows",
                failed_poll_attempts,
                total_rows);
            return true;
        }
        return false;
    };

    auto maybe_guard = consumer.poll(msg_sink);

    // Return empty optional if the consumer was unable to poll any messages or the transformation of those messages resulted in no rows
    if (!maybe_guard.has_value() || total_rows == 0)
        return {};

    /// MATERIALIZED columns can be added here, but I think
    // they are not needed here:
    // and it's misleading to use them here,
    // as columns 'materialized' that way stays 'ephemeral'
    // i.e. will not be stored anywhere
    // If needed any extra columns can be added using DEFAULT they can be added at MV level if needed.

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    BlocksList blocks;
    blocks.emplace_back(std::move(result_block));
    return BlocksAndGuard{std::move(blocks), std::move(*maybe_guard)};
}

void StorageKafka2::threadFunc(size_t idx)
{
    chassert(idx < tasks.size());
    auto task = tasks[idx];
    std::optional<StallKind> maybe_stall_reason;
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
        if (num_views)
        {
            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled && num_created_consumers > 0)
            {
                maybe_stall_reason.reset();
                if (!StorageKafkaUtils::checkDependencies(table_id, getContext()))
                {
                    ProfileEvents::increment(ProfileEvents::KafkaMVNotReady);
                    break;
                }

                LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

                // Exit the loop & reschedule if some stream stalled
                if (maybe_stall_reason = streamToViews(idx); maybe_stall_reason.has_value())
                {
                    LOG_TRACE(
                        log,
                        "Stream stalled. Rescheduling in {} ms",
                        (*kafka_settings)[KafkaSetting::kafka_consumer_reschedule_ms].totalMilliseconds());
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
                if (duration.count() > KAFKA_MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(
                        log,
                        "Thread work duration limit exceeded. Rescheduling in {} ms",
                        (*kafka_settings)[KafkaSetting::kafka_consumer_reschedule_ms].totalMilliseconds());
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (!task->stream_cancelled)
    {
        UInt64 kafka_consumer_reschedule_ms = (*kafka_settings)[KafkaSetting::kafka_consumer_reschedule_ms].totalMilliseconds();
        if (maybe_stall_reason.has_value() && *maybe_stall_reason == StallKind::ShortStall)
            task->holder->scheduleAfter(kafka_consumer_reschedule_ms / 10);
        else
            task->holder->scheduleAfter(kafka_consumer_reschedule_ms);
    }
}

std::optional<StorageKafka2::StallKind> StorageKafka2::streamToViews(size_t idx)
{
    auto component_guard = Coordination::setCurrentComponent("StorageKafka2::streamToViews");
    // This function is written assuming that each consumer has their own thread. This means once this is changed, this
    // function should be revisited. The return values should be revisited, as stalling all consumers because of a
    // single one stalled is not a good idea.
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaBackgroundReads};
    ProfileEvents::increment(ProfileEvents::KafkaBackgroundReads);

    auto consumer = acquireConsumer(idx);

    SCOPE_EXIT({ releaseConsumer(std::move(consumer)); });

    Stopwatch watch{CLOCK_MONOTONIC_COARSE};

    try
    {
        if (consumer->needsNewKeeper())
            consumer->setKeeper(getZooKeeperAndAssertActive());

        if (const auto cannot_poll_reason = consumer->prepareToPoll(); cannot_poll_reason.has_value())
            return getStallKind(*cannot_poll_reason);

        LOG_TRACE(log, "Trying to consume from consumer {}", idx);
        const auto maybe_rows = streamFromConsumer(*consumer, watch);
        if (maybe_rows.has_value())
        {
            const auto milliseconds = watch.elapsedMilliseconds();
            LOG_DEBUG(
                log, "Pushing {} rows took {} ms.", formatReadableQuantity(*maybe_rows), milliseconds);
        }
        else
        {
            LOG_DEBUG(log, "Couldn't stream any messages");
            return StallKind::LongStall;
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (Coordination::isHardwareError(e.code))
        {
            LOG_INFO(log, "Cleaning up topic-partitions locks because of exception: {}", e.displayText());
            activating_task->schedule();
            // Keeper sessions should be restored fast, so let's try to poll again sooner
            return StallKind::ShortStall;
        }

        throw;
    }
    return {};
}

StorageKafka2::KeeperHandlingConsumerPtr StorageKafka2::acquireConsumer(size_t idx)
{
    std::lock_guard lock{consumers_mutex};
    if (idx >= consumers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid consumer index: {}, number of consumers is {}", idx, consumers.size());

    auto consumer = consumers[idx];
    const auto created_consumer = consumer->startUsing([&](IKafkaExceptionInfoSinkPtr exception_sink)
                                                       { return getConsumerConfiguration(idx, std::move(exception_sink)); });

    if (created_consumer)
        LOG_TRACE(log, "Created #{} consumer", idx);

    CurrentMetrics::add(CurrentMetrics::KafkaConsumersInUse);

    return consumer;
}

void StorageKafka2::releaseConsumer(KeeperHandlingConsumerPtr && consumer_ptr)
{
    std::lock_guard lock{consumers_mutex};
    consumer_ptr->stopUsing();
    cv.notify_one();
    CurrentMetrics::sub(CurrentMetrics::KafkaConsumersInUse);
}

void StorageKafka2::cleanConsumers()
{
    /// We need to clear the cppkafka::Consumer separately from KafkaConsumer2, since cppkafka::Consumer holds a
    /// weak_ptr to the KafkaConsumer2 (for logging and stat callback). So if we destroy cppkafka::Consumer in
    /// KafkaConsumer2 destructor, then due to librdkafka will call the logging again from destructor, it will lead to a
    /// deadlock. Maybe we could do this in the destructor of KeeperHandlingConsumer, thus avoid this not obvious logic
    /// here, but this version is "battle tested" by our CI as we have the very similar, if not the same approach in the
    /// old StorageKafka. Let's go with this now, later on we can improve it.
    std::vector<CppKafkaConsumerPtr> cpp_consumers_to_close;
    {
        UniqueLock lock(consumers_mutex);
        /// Wait until all consumers will be released
        /// Clang Thread Safety Analysis doesn't understand std::condition_variable::wait and std::unique_lock
        cv.wait(
            lock.getUnderlyingLock(),
            [&, this]() TSA_NO_THREAD_SAFETY_ANALYSIS
            {
                auto it = std::find_if(consumers.begin(), consumers.end(), [](const auto & ptr) { return ptr->isInUse(); });
                return it == consumers.end();
            });

        for (const auto & consumer : consumers)
        {
            if (!consumer->hasConsumer())
                continue;
            cpp_consumers_to_close.push_back(consumer->moveConsumer());
        }
    }

    cpp_consumers_to_close.clear();

    std::lock_guard lock(consumers_mutex);
    consumers.clear();
}

std::optional<size_t> StorageKafka2::streamFromConsumer(KeeperHandlingConsumer & consumer, const Stopwatch & watch)
{
    // Create an INSERT query for streaming data
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = getStorageID();

    auto modified_context = Context::createCopy(getContext());
    modified_context->makeQueryContext();
    modified_context->applySettingsChanges(settings_adjustments);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        modified_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);
    auto block_io = interpreter.execute();

    auto maybe_blocks_and_guard = pollConsumer(consumer, watch, modified_context);

    if (!maybe_blocks_and_guard.has_value() || maybe_blocks_and_guard->blocks.empty())
    {
        if (maybe_blocks_and_guard.has_value())
        {
            LOG_TRACE(log, "No rows to insert");
            maybe_blocks_and_guard->guard.commit();
        }
        else
        {
            LOG_TRACE(log, "Didn't get any messages");
        }
        block_io.onCancelOrConnectionLoss();
        return std::nullopt;
    }

    auto [blocks, offset_guard] = std::move(*maybe_blocks_and_guard);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        blocks.front().cloneEmpty().getColumnsWithTypeAndName(),
        block_io.pipeline.getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        modified_context);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));

    for (auto & block : blocks)
        converting_actions->execute(block);

    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(Pipe{std::make_shared<BlocksListSource>(std::move(blocks))});

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    offset_guard.commit();

    return rows;
}

void StorageKafka2::setZooKeeper()
{
    std::unique_lock lock{keeper_mutex};
    keeper = getContext()->getZooKeeper();
}

zkutil::ZooKeeperPtr StorageKafka2::tryGetZooKeeper() const
{
    std::unique_lock lock{keeper_mutex};
    return keeper;
}

zkutil::ZooKeeperPtr StorageKafka2::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot get ZooKeeper");
    return res;
}

zkutil::ZooKeeperPtr StorageKafka2::getZooKeeperAndAssertActive() const
{
    auto res = getZooKeeper();
    assertActive();
    return res;
}

zkutil::ZooKeeperPtr StorageKafka2::getZooKeeperIfTableShutDown() const
{
    zkutil::ZooKeeperPtr new_zookeeper = getContext()->getZooKeeper();
    new_zookeeper->sync(keeper_path);
    return new_zookeeper;
}

StorageKafka2::StallKind StorageKafka2::getStallKind(const KeeperHandlingConsumer::CannotPollReason & reason)
{
    /// Keeper session should be restored fast, therefore we don't want to stall the stream for too long because of that.
    switch (reason)
    {
        case KeeperHandlingConsumer::CannotPollReason::NoPartitions:
            [[fallthrough]];
        case KeeperHandlingConsumer::CannotPollReason::NoMetadata:
            return StallKind::LongStall;
        case KeeperHandlingConsumer::CannotPollReason::KeeperSessionEnded:
            return StallKind::ShortStall;
    }
}
}
