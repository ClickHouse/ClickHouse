#include <Storages/Kafka/StorageKafka2.h>

#include <Columns/IColumn.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateConstantExpression.h>
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
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
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

#include <filesystem>
#include <string>

namespace CurrentMetrics
{
// TODO: Add proper metrics, similar to old StorageKafka
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
}


namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
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
constexpr auto TMP_LOCKS_REFRESH_POLLS = 15;
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
    , semaphore(0, static_cast<int>(num_consumers))
    , settings_adjustments(StorageKafkaUtils::createSettingsAdjustments(*kafka_settings, schema_name))
    , thread_per_consumer((*kafka_settings)[KafkaSetting::kafka_thread_per_consumer].value)
    , collection_name(collection_name_)
    , active_node_identifier(toString(ServerUUID::get()))
{
    if ((*kafka_settings)[KafkaSetting::kafka_num_consumers] > 1 && !thread_per_consumer)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "With multiple consumers, it is required to use `kafka_thread_per_consumer` setting");

    if ((*kafka_settings)[KafkaSetting::kafka_handle_error_mode] == StreamingHandleErrorMode::STREAM)
    {
        (*kafka_settings)[KafkaSetting::input_format_allow_errors_num] = 0;
        (*kafka_settings)[KafkaSetting::input_format_allow_errors_ratio] = 0;
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(StorageKafkaUtils::createVirtuals((*kafka_settings)[KafkaSetting::kafka_handle_error_mode]));

    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i] { threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }

    const auto first_replica = createTableIfNotExists();

    if (!first_replica)
        createReplica();

    activating_task = getContext()->getSchedulePool().createTask(log->name() + "(activating task)", [this]() { activateAndReschedule(); });
    activating_task->deactivate();
}

StorageKafka2::~StorageKafka2() = default;

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

    /// It would be ideal to introduce a setting for this
    constexpr static size_t check_period_ms = 60000;
    /// In case of any exceptions we want to rerun the this task as fast as possible but we also don't want to keep retrying immediately
    /// in a close loop (as fast as tasks can be processed), so we'll retry in between 100 and 10000 ms
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
    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), topics[0], std::chrono::milliseconds(poll_timeout), shutdown_called, header);

    LOG_TRACE(log, "Kafka producer created");

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(header, getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}

void StorageKafka2::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            consumers.push_back(ConsumerAndAssignmentInfo{.consumer = createConsumer(i), .keeper = getZooKeeper()});
            LOG_DEBUG(log, "Created #{} consumer", num_created_consumers);
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }
    activating_task->activateAndSchedule();
}


void StorageKafka2::shutdown(bool)
{
    shutdown_called = true;
    activating_task->deactivate();
    partialShutdown();
    LOG_TRACE(log, "Closing consumers");
    consumers.clear();
    LOG_TRACE(log, "Consumers closed");
}

void StorageKafka2::drop()
{
    dropReplica();
}

KafkaConsumer2Ptr StorageKafka2::createConsumer(size_t consumer_number)
{
    // Create a consumer and subscribe to topics
    auto consumer_impl = std::make_shared<cppkafka::Consumer>(getConsumerConfiguration(consumer_number));
    consumer_impl->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    chassert((thread_per_consumer || num_consumers == 1) && "StorageKafka2 cannot handle multiple consumers on a single thread");
    auto & stream_cancelled = tasks[consumer_number]->stream_cancelled;
    return std::make_shared<KafkaConsumer2>(
        consumer_impl, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), stream_cancelled, topics);

}


cppkafka::Configuration StorageKafka2::getConsumerConfiguration(size_t consumer_number)
{
    KafkaConfigLoader::ConsumerConfigParams params{
        {getContext()->getConfigRef(), collection_name, topics, log},
        brokers,
        group,
        num_consumers > 1,
        consumer_number,
        client_id,
        getMaxBlockSize()};
    auto kafka_config = KafkaConfigLoader::getConsumerConfiguration(*this, params);
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

namespace
{
const std::string lock_file_name{"lock"};
const std::string commit_file_name{"committed"};
const std::string intent_file_name{"intention"};

std::optional<int64_t> getNumber(zkutil::ZooKeeper & keeper, const fs::path & path)
{
    std::string result;
    if (!keeper.tryGet(path, result))
        return std::nullopt;

    return DB::parse<int64_t>(result);
}
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

        // Save all locked partitions
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
    // TODO: This can cause issues if a new table is created with the same path. To make this work, we should store some metadata
    // about the table to be able to identify that the same table is created, not a new one.
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

    /** At this moment, another replica can be created and we cannot remove the table.
      * Try to remove /replicas node first. If we successfully removed it,
      * it guarantees that we are the only replica that proceed to remove the table
      * and no new replicas can be created after that moment (it requires the existence of /replicas node).
      * and table cannot be recreated with new /replicas node on another servers while we are removing data,
      * because table creation is executed in single transaction that will conflict with remaining nodes.
      */

    /// Node /dropped works like a lock that protects from concurrent removal of old table and creation of new table.
    /// But recursive removal may fail in the middle of operation leaving some garbage in zookeeper_path, so
    /// we remove it on table creation if there is /dropped node. Creating thread may remove /dropped node created by
    /// removing thread, and it causes race condition if removing thread is not finished yet.
    /// To avoid this we also create ephemeral child before starting recursive removal.
    /// (The existence of child node does not allow to remove parent node).
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


// We go through all the topic partitions, count the number of live replicas,
// and see which partitions are already locked by other replicas
std::pair<StorageKafka2::TopicPartitionSet, StorageKafka2::ActiveReplicasInfo> StorageKafka2::getLockedTopicPartitions(zkutil::ZooKeeper & keeper_to_use)
{
    LOG_TRACE(log, "Starting to lookup replica's state");
    StorageKafka2::TopicPartitionSet locked_partitions;
    auto lock_nodes = keeper_to_use.getChildren(keeper_path + "/topic_partition_locks");
    std::unordered_set<String> replicas_with_lock;

    for (const auto & lock_name : lock_nodes)
    {
        replicas_with_lock.insert(keeper_to_use.get(keeper_path + "/topic_partition_locks/" + lock_name));
        auto base = lock_name.substr(0, lock_name.size() - 5); // drop ".lock"
        auto sep  = base.rfind('_');
        if (sep == String::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Topic partition lock path {} is not in correct format.", lock_name);

        String topic = base.substr(0, sep);
        Int32  partition = parse<Int32>(base.substr(sep + 1));
        TopicPartition topic_partition
        {
            .topic = topic,
            .partition_id = partition,
            .offset = KafkaConsumer2::INVALID_OFFSET
        };

        locked_partitions.insert(topic_partition);
    }

    Strings already_locked_partitions_str;
    already_locked_partitions_str.reserve(locked_partitions.size());
    for (const auto & already_locks : locked_partitions)
        already_locked_partitions_str.push_back(fmt::format("[{}:{}]", already_locks.topic, already_locks.partition_id));
    LOG_INFO(
        log,
        "Already locked topic partitions are [{}]",
        boost::algorithm::join(already_locked_partitions_str, ", ")
    );

    const auto replicas_count = keeper_to_use.getChildren(keeper_path + "/replicas").size();
    LOG_TEST(log, "There are {} replicas with lock and there are {} replicas in total", replicas_with_lock.size(), replicas_count);
    const auto has_replica_without_locks = replicas_with_lock.size() < replicas_count;
    return {locked_partitions, ActiveReplicasInfo{replicas_count, has_replica_without_locks}};
}

std::pair<StorageKafka2::TopicPartitions, StorageKafka2::ActiveReplicasInfo> StorageKafka2::getAvailableTopicPartitions(zkutil::ZooKeeper & keeper_to_use, const TopicPartitions & all_topic_partitions)
{
    const auto get_locked_partitions_res = getLockedTopicPartitions(keeper_to_use);
    const auto & already_locked_partitions = get_locked_partitions_res.first;
    TopicPartitions available_topic_partitions;
    available_topic_partitions.reserve(all_topic_partitions.size());
    for (const auto & partition : all_topic_partitions)
    {
        if (!already_locked_partitions.contains(partition))
            available_topic_partitions.push_back(partition);
    }
    Strings available_topic_partitions_str;
    available_topic_partitions_str.reserve(available_topic_partitions.size());
    for (const auto & available_partition : available_topic_partitions)
        available_topic_partitions_str.push_back(fmt::format("[{}:{}]", available_partition.topic, available_partition.partition_id));
    LOG_INFO(
        log,
        "Topic partitions [{}] are available to lock",
        boost::algorithm::join(available_topic_partitions_str, ", ")
    );

    return {available_topic_partitions, get_locked_partitions_res.second};
}

std::optional<StorageKafka2::LockedTopicPartitionInfo> StorageKafka2::createLocksInfoIfFree(zkutil::ZooKeeper & keeper_to_use, const TopicPartition& partition_to_lock)
{
    const auto topic_partition_path = getTopicPartitionPath(partition_to_lock);
    const auto lock_file_path = getTopicPartitionLockPath(partition_to_lock);
    keeper_to_use.createAncestors(lock_file_path);
    try
    {
        using zkutil::EphemeralNodeHolder;
        LockedTopicPartitionInfo lock_info{
            EphemeralNodeHolder::create(lock_file_path, keeper_to_use, (*kafka_settings)[KafkaSetting::kafka_replica_name].value),
            getNumber(keeper_to_use, topic_partition_path / commit_file_name),
            getNumber(keeper_to_use, topic_partition_path / intent_file_name)};

        LOG_TRACE(
            log,
            "Locked topic partition: {}:{} at offset {} with intent size {}, offset present: {}, intent size present: {}",
            partition_to_lock.topic,
            partition_to_lock.partition_id,
            lock_info.committed_offset.value_or(0),
            lock_info.intent_size.value_or(0),
            lock_info.committed_offset.has_value(),
            lock_info.intent_size.has_value());

        return lock_info;
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_TRACE(
                log,
                "Skipping lock for topic partition {}:{} because it already exists",
                partition_to_lock.topic, partition_to_lock.partition_id);
            return std::nullopt;
        }
        throw;
    }
}

void StorageKafka2::lockTemporaryLocks(zkutil::ZooKeeper & keeper_to_use, const TopicPartitions & available_topic_partitions, TopicPartitionLocks & tmp_locks, size_t & tmp_locks_quota, const bool has_replica_without_locks)
{
    /// There are no available topic partitions, so drop the quota to 0
    if (available_topic_partitions.empty())
    {
        LOG_TRACE(log, "There are no available topic partitions to lock");
        tmp_locks_quota = 0;
        return;
    }
    LOG_TRACE(log, "Starting to lock temporary locks");

    /// We have some temporary lock quota, but there is at least one replica without locks, let's drop the quote to give the other replica a chance to lock some partitions
    if (tmp_locks_quota > 0 && has_replica_without_locks)
    {
        LOG_TRACE(log, "There is at least one consumer without locks, won't lock any temporary locks this round");
        tmp_locks_quota = 0;
        return;
    }

    /// We have some temporary lock quota, but it is greater than the number of available topic partitions,
    /// so we will reduce the quota to give other replicas a chance to lock some partitions
    if (tmp_locks_quota > 0 && tmp_locks_quota <= available_topic_partitions.size())
    {

        LOG_TRACE(log, "Reducing temporary locks to give other replicas a chance to lock some partitions");
        tmp_locks_quota = std::min(tmp_locks_quota - 1, available_topic_partitions.size() - 1);
    }
    else
    {
        tmp_locks_quota = std::min(available_topic_partitions.size(), tmp_locks_quota + 1);
    }
    LOG_INFO(log, "The replica can take {} temporary locks in the current round", tmp_locks_quota);

    if (tmp_locks_quota == 0)
        return;

    auto available_topic_partitions_copy = available_topic_partitions;
    pcg64 generator(randomSeed());
    std::shuffle(available_topic_partitions_copy.begin(), available_topic_partitions_copy.end(), generator);

    for (const auto & tp : available_topic_partitions)
    {
        if (tmp_locks.size() >= tmp_locks_quota)
            break;
        auto maybe_lock = createLocksInfoIfFree(keeper_to_use, tp);
        if (!maybe_lock.has_value())
            continue;
        tmp_locks.emplace(TopicPartition(tp), std::move(*maybe_lock));
    }
}

// If the number of locks on a replica is greater than it can hold, then we first release the partitions that we can no longer hold.
// Otherwise, we try to lock free partitions one by one.
void StorageKafka2::updatePermanentLocks(zkutil::ZooKeeper & keeper_to_use, const TopicPartitions & available_topic_partitions, TopicPartitionLocks & permanent_locks, const size_t topic_partitions_count, const size_t active_replica_count)
{
    LOG_TRACE(log, "Starting to update permanent locks");
    chassert(active_replica_count > 0 && "There should be at least one active replica, because we are active");
    size_t can_lock_partitions = std::max<size_t>(topic_partitions_count / static_cast<size_t>(active_replica_count), 1);

    LOG_TRACE(log, "The replica can have {} permanent locks after the current round", can_lock_partitions);

    if (can_lock_partitions == permanent_locks.size())
    {
        LOG_TRACE(log, "The number of permanent locks is equal to the number of locks that can be taken, will not update them");
        return;
    }

    if (can_lock_partitions < permanent_locks.size())
    {
        LOG_TRACE(log, "Will release the extra {} topic partition locks", permanent_locks.size() - can_lock_partitions);
        size_t need_to_unlock = permanent_locks.size() - can_lock_partitions;
        auto permanent_locks_it = permanent_locks.begin();
        for (size_t i = 0; i < need_to_unlock && permanent_locks_it != permanent_locks.end(); ++i)
        {
            LOG_TEST(log, "Releasing topic partition lock for [{}:{}] at offset",
                permanent_locks_it->first.topic, permanent_locks_it->first.partition_id);
            permanent_locks_it = permanent_locks.erase(permanent_locks_it);
        }
    }
    else
    {
        size_t need_to_lock = can_lock_partitions - permanent_locks.size();
        LOG_TRACE(log, "Will try to lock {} topic partitions", need_to_lock);
        auto tp_it = available_topic_partitions.begin();
        for (size_t i = 0; i < need_to_lock && tp_it != available_topic_partitions.end();)
        {
            const auto &tp = *tp_it;
            ++tp_it;
            auto maybe_lock = createLocksInfoIfFree(keeper_to_use, tp);
            if (!maybe_lock.has_value())
                continue;
            permanent_locks.emplace(TopicPartition(tp), std::move(*maybe_lock));
            ++i;
        }
    }
}

void StorageKafka2::saveTopicPartitionInfo(zkutil::ZooKeeper & keeper_to_use, const std::filesystem::path & keeper_path_to_data, const String & data)
{
    Coordination::Requests ops;
    keeper_to_use.checkExistsAndGetCreateAncestorsOps(keeper_path_to_data, ops);
    if (keeper_to_use.exists(keeper_path_to_data))
        ops.emplace_back(zkutil::makeSetRequest(keeper_path_to_data, data, -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(keeper_path_to_data, data, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    const auto code = keeper_to_use.tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);
}

void StorageKafka2::saveCommittedOffset(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition)
{
    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    saveTopicPartitionInfo(keeper_to_use, partition_prefix / commit_file_name, toString(topic_partition.offset));
    // This is best effort, if it fails we will try to remove in the next round
    keeper_to_use.tryRemove(partition_prefix / intent_file_name, -1);
    LOG_TEST(
        log, "Saved offset {} for topic-partition [{}:{}]", topic_partition.offset, topic_partition.topic, topic_partition.partition_id);
}

void StorageKafka2::saveIntent(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition, int64_t intent)
{
    if (intent <= 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Intent for topic-partition [{}:{}] must be greater than 0, but got {}",
            topic_partition.topic,
            topic_partition.partition_id,
            intent);

    LOG_TEST(
        log,
        "Saving intent of {} for topic-partition [{}:{}] at offset {}",
        intent,
        topic_partition.topic,
        topic_partition.partition_id,
        topic_partition.offset);

    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    saveTopicPartitionInfo(keeper_to_use, partition_prefix / intent_file_name, toString(intent));
}


StorageKafka2::PolledBatchInfo StorageKafka2::pollConsumer(
    KafkaConsumer2 & consumer,
    const TopicPartition & topic_partition,
    std::optional<int64_t> message_count,
    Stopwatch & total_stopwatch,
    const ContextPtr & modified_context)
{
    LOG_TEST(log, "Polling consumer");
    PolledBatchInfo batch_info;
    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    Block non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized());
    auto virtual_header = getVirtualsHeader();

    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto put_error_to_stream = (*kafka_settings)[KafkaSetting::kafka_handle_error_mode] == StreamingHandleErrorMode::STREAM;

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        getFormatName(), empty_buf, non_virtual_header, modified_context, getMaxBlockSize(), std::nullopt, 1);

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t intent_size = 0;
    size_t failed_poll_attempts = 0;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        if (put_error_to_stream)
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

        e.addMessage(
            "while parsing Kafka message (topic: {}, partition: {}, offset: {})'",
            consumer.currentTopic(),
            consumer.currentPartition(),
            consumer.currentOffset());
        throw std::move(e);
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));


    Poco::Timespan max_execution_time = (*kafka_settings)[KafkaSetting::kafka_flush_interval_ms].changed
        ? (*kafka_settings)[KafkaSetting::kafka_flush_interval_ms]
        : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

    const auto check_time_limit = [&max_execution_time, &total_stopwatch]()
    {
        if (max_execution_time != 0)
        {
            auto elapsed_ns = total_stopwatch.elapsed();

            if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
                return false;
        }

        return true;
    };

    while (true)
    {
        size_t new_rows = 0;
        exception_message.reset();
        if (auto buf = consumer.consume(topic_partition, message_count))
        {
            ++intent_size;
            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);
            new_rows = executor.execute(*buf);
        }

        if (new_rows)
        {
            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

            const auto & header_list = consumer.currentHeaderList();

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
                virtual_columns[0]->insert(consumer.currentTopic());
                virtual_columns[1]->insert(consumer.currentKey());
                virtual_columns[2]->insert(consumer.currentOffset());
                virtual_columns[3]->insert(consumer.currentPartition());


                auto timestamp_raw = consumer.currentTimestamp();
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
                if (put_error_to_stream)
                {
                    if (exception_message)
                    {
                        virtual_columns[8]->insert(consumer.currentPayload());
                        virtual_columns[9]->insert(*exception_message);
                    }
                    else
                    {
                        virtual_columns[8]->insertDefault();
                        virtual_columns[9]->insertDefault();
                    }
                }
            }

            total_rows = total_rows + new_rows;
            batch_info.last_offset = consumer.currentOffset();
        }
        else if (consumer.isStalled())
        {
            ++failed_poll_attempts;
        }
        else
        {
            // We came here in case of tombstone (or sometimes zero-length) messages, and it is not something abnormal
            // TODO: it seems like in case of put_error_to_stream=true we may need to process those differently
            // currently we just skip them with note in logs.
            LOG_DEBUG(
                log,
                "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
                consumer.currentTopic(),
                consumer.currentPartition(),
                consumer.currentOffset());
        }

        if (!consumer.hasMorePolledMessages()
            && (total_rows >= getMaxBlockSize() || !check_time_limit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            LOG_TRACE(
                log,
                "Stopped collecting message for current batch. There are {} failed polled attempts, {} total rows",
                failed_poll_attempts,
                total_rows);
            break;
        }
    }

    if (total_rows == 0)
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

    batch_info.blocks.emplace_back(std::move(result_block));
    batch_info.intent_size = intent_size;
    return batch_info;
}

void StorageKafka2::threadFunc(size_t idx)
{
    chassert(idx < tasks.size());
    auto task = tasks[idx];
    std::optional<StallReason> maybe_stall_reason;
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
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

                // Exit the loop & reschedule if some stream stalled
                if (maybe_stall_reason = streamToViews(idx); maybe_stall_reason.has_value())
                {
                    LOG_TRACE(log, "Stream stalled.");
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
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
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (!task->stream_cancelled)
    {
        // Keeper related problems should be solved relatively fast, it makes sense wait less time
        if (maybe_stall_reason.has_value()
            && (*maybe_stall_reason == StallReason::KeeperSessionEnded || *maybe_stall_reason == StallReason::CouldNotAcquireLocks || *maybe_stall_reason == StallReason::NoMetadata))
            task->holder->scheduleAfter(KAFKA_RESCHEDULE_MS / 10);
        else
            task->holder->scheduleAfter(KAFKA_RESCHEDULE_MS);
    }
}

std::optional<StorageKafka2::StallReason> StorageKafka2::streamToViews(size_t idx)
{
    // This function is written assuming that each consumer has their own thread. This means once this is changed, this function should be revisited.
    // The return values should be revisited, as stalling all consumers because of a single one stalled is not a good idea.
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaBackgroundReads};
    ProfileEvents::increment(ProfileEvents::KafkaBackgroundReads);

    auto & consumer_info = consumers[idx];
    consumer_info.watch.restart();
    auto & consumer = consumer_info.consumer;
    LOG_TRACE(log, "Polling consumer {} for events", idx);
    consumer->pollEvents();

    try
    {
        if (consumer_info.permanent_locks.empty() || consumer_info.poll_count >= TMP_LOCKS_REFRESH_POLLS)
        {
            consumer_info.topic_partitions.clear();

            consumer_info.consume_from_topic_partition_index = 0;

            if (consumer_info.keeper->expired())
            {
                consumer_info.keeper = getZooKeeperAndAssertActive();
                LOG_TEST(log, "Got new zookeeper");
            }

            auto all_topic_partitions = consumer->getAllTopicPartitions();
            if (all_topic_partitions.empty())
            {
                LOG_DEBUG(log, "Couldn't get list of all topic partitions");
                return StallReason::NoMetadata;
            }

            // Clear temporary locks to give a chance to lock them as permanent locks and to make it possible to gather available topic partitions only once.
            consumer_info.tmp_locks.clear();
            const auto [available_topic_partitions, active_replicas_info] = getAvailableTopicPartitions(*consumer_info.keeper, all_topic_partitions);
            updatePermanentLocks(*consumer_info.keeper, available_topic_partitions, consumer_info.permanent_locks, all_topic_partitions.size(), active_replicas_info.active_replica_count);
            lockTemporaryLocks(*consumer_info.keeper, available_topic_partitions, consumer_info.tmp_locks, consumer_info.tmp_locks_quota, active_replicas_info.has_replica_without_locks);
            consumer_info.poll_count = 0;

            // Now we always have some assignment
            consumer_info.topic_partitions.clear();
            consumer_info.topic_partitions.reserve(consumer_info.permanent_locks.size() + consumer_info.tmp_locks.size());
            auto update_topic_partitions = [&](const auto & locks)
            {
                for (const auto & [topic_partition, info] : locks)
                {
                    TopicPartition copy = topic_partition;
                    if (info.committed_offset.has_value())
                        copy.offset = *info.committed_offset;
                    consumer_info.topic_partitions.push_back(std::move(copy));
                }
            };
            update_topic_partitions(consumer_info.permanent_locks);
            update_topic_partitions(consumer_info.tmp_locks);
            consumer->updateOffsets(consumer_info.topic_partitions);
        }

        if (consumer_info.topic_partitions.empty())
        {
            LOG_TRACE(log, "Consumer {} has assignment, but has no partitions, probably because there are more consumers in the consumer group than partitions.", idx);
            return StallReason::NoPartitions;
        }
        LOG_TRACE(log, "Trying to consume from consumer {}", idx);
        const auto maybe_rows = streamFromConsumer(consumer_info);
        if (maybe_rows.has_value())
        {
            const auto milliseconds = consumer_info.watch.elapsedMilliseconds();
            LOG_DEBUG(
                log, "Pushing {} rows to {} took {} ms.", formatReadableQuantity(*maybe_rows), table_id.getNameForLogs(), milliseconds);
        }
        else
        {
            LOG_DEBUG(log, "Couldn't stream any messages");
            return StallReason::NoMessages;
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (Coordination::isHardwareError(e.code))
        {
            LOG_INFO(log, "Cleaning up topic-partitions locks because of exception: {}", e.displayText());
            consumer_info.tmp_locks.clear();
            consumer_info.permanent_locks.clear();
            activating_task->schedule();
            return StallReason::KeeperSessionEnded;
        }

        throw;
    }
    return {};
}


std::optional<size_t> StorageKafka2::streamFromConsumer(ConsumerAndAssignmentInfo & consumer_info)
{
    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = getStorageID();

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

    auto & topic_partition = consumer_info.topic_partitions[consumer_info.consume_from_topic_partition_index];
    LOG_TRACE(
        log,
        "Will fetch {}:{} (consume_from_topic_partition_index is {})",
        topic_partition.topic,
        topic_partition.partition_id,
        consumer_info.consume_from_topic_partition_index);
    consumer_info.consume_from_topic_partition_index
        = (consumer_info.consume_from_topic_partition_index + 1) % consumer_info.topic_partitions.size();

    bool needs_offset_reset = true;
    SCOPE_EXIT({
        if (!needs_offset_reset)
            return;
        consumer_info.consumer->updateOffsets(consumer_info.topic_partitions);
    });

    auto & keeper_to_use = *consumer_info.keeper;

    // Change temporary locks every TMP_LOCKS_REFRESH_POLLS calls (or whenever permanent_locks_changed is set)
    // This keeps batchy work from sitting too long on the same partitions and smoothly hands off load over time.
    ++consumer_info.poll_count;

    auto * lock_info = consumer_info.findTopicPartitionLock(topic_partition);
    auto [blocks, intent_size, last_read_offset] = pollConsumer(
        *consumer_info.consumer, topic_partition, lock_info->intent_size, consumer_info.watch, kafka_context);

    if (blocks.empty())
    {
        LOG_TRACE(log, "Didn't get any messages");
        needs_offset_reset = false;
        block_io.onCancelOrConnectionLoss();
        return std::nullopt;
    }

    auto converting_dag = ActionsDAG::makeConvertingActions(
        blocks.front().cloneEmpty().getColumnsWithTypeAndName(),
        block_io.pipeline.getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));

    for (auto & block : blocks)
        converting_actions->execute(block);

    // We can't cancel during copyData, as it's not aware of commits and other kafka-related stuff.
    // It will be cancelled on underlying layer (kafka buffer)
    lock_info->intent_size = intent_size;
    saveIntent(keeper_to_use, topic_partition, *lock_info->intent_size);
    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(Pipe{std::make_shared<BlocksListSource>(std::move(blocks))});

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    lock_info->committed_offset = last_read_offset + 1;
    topic_partition.offset = last_read_offset + 1;
    saveCommittedOffset(keeper_to_use, topic_partition);
    consumer_info.consumer->commit(topic_partition);
    lock_info->intent_size.reset();
    needs_offset_reset = false;

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

fs::path StorageKafka2::getTopicPartitionPath(const TopicPartition & topic_partition)
{
    return fs_keeper_path / "topics" / topic_partition.topic / "partitions" / std::to_string(topic_partition.partition_id);
}

fs::path StorageKafka2::getTopicPartitionLockPath(const TopicPartition & topic_partition)
{
    auto topic_partition_name = fmt::format("{}_{}.{}", topic_partition.topic, topic_partition.partition_id, lock_file_name);
    return fs_keeper_path / "topic_partition_locks" / topic_partition_name;
}

}
