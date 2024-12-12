#include <Storages/Kafka/StorageKafka2.h>

#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
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
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <librdkafka/rdkafka.h>

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
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
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
constexpr auto MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS = 15000;
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
    , keeper_path(kafka_settings_->kafka_keeper_path.value)
    , replica_path(keeper_path + "/replicas/" + kafka_settings_->kafka_replica_name.value)
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
    , log(getLogger("StorageKafka2 (" + table_id_.getNameForLogs() + ")"))
    , semaphore(0, static_cast<int>(num_consumers))
    , settings_adjustments(StorageKafkaUtils::createSettingsAdjustments(*kafka_settings, schema_name))
    , thread_per_consumer(kafka_settings->kafka_thread_per_consumer.value)
    , collection_name(collection_name_)
    , active_node_identifier(toString(ServerUUID::get()))
{
    if (kafka_settings->kafka_num_consumers > 1 && !thread_per_consumer)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "With multiple consumers, it is required to use `kafka_thread_per_consumer` setting");

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

            consumers.back().consumer->subscribeIfNotSubscribedYet();
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
    return kafka_settings->kafka_max_block_size.changed ? kafka_settings->kafka_max_block_size.value
                                                        : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}

size_t StorageKafka2::getPollMaxBatchSize() const
{
    size_t batch_size = kafka_settings->kafka_poll_max_batch_size.changed ? kafka_settings->kafka_poll_max_batch_size.value
                                                                          : getContext()->getSettingsRef()[Setting::max_block_size].value;

    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageKafka2::getPollTimeoutMillisecond() const
{
    return kafka_settings->kafka_poll_timeout_ms.changed ? kafka_settings->kafka_poll_timeout_ms.totalMilliseconds()
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
    const auto my_keeper_path = fs::path(keeper_path);
    const auto replicas_path = my_keeper_path / "replicas";

    for (auto i = 0; i < 1000; ++i)
    {
        if (keeper->exists(replicas_path))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", keeper_path);
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (keeper->exists(my_keeper_path / "dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {}", keeper_path);
            String drop_lock_path = my_keeper_path / "dropped" / "lock";
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

        const auto topics_path = my_keeper_path / "topics";
        ops.emplace_back(zkutil::makeCreateRequest(topics_path, "", zkutil::CreateMode::Persistent));

        for (const auto & topic : topics)
        {
            LOG_DEBUG(log, "Creating path in keeper for topic {}", topic);

            const auto topic_path = topics_path / topic;
            ops.emplace_back(zkutil::makeCreateRequest(topic_path, "", zkutil::CreateMode::Persistent));

            const auto partitions_path = topic_path / "partitions";
            ops.emplace_back(zkutil::makeCreateRequest(partitions_path, "", zkutil::CreateMode::Persistent));
        }

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
        else if (code != Coordination::Error::ZOK)
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

    const auto my_keeper_path = fs::path(keeper_path);
    for (const auto & child : children)
        if (child != "dropped")
            keeper_to_use->tryRemoveRecursive(my_keeper_path / child);

    Coordination::Requests ops;
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(my_keeper_path / "dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(my_keeper_path, -1));
    const auto code = keeper_to_use->tryMulti(ops, responses, /* check_session_valid */ true);

    if (code == Coordination::Error::ZNONODE)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
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
    fs::path my_keeper_path = keeper_path;
    String drop_lock_path = my_keeper_path / "dropped" / "lock";
    ops.emplace_back(zkutil::makeRemoveRequest(my_keeper_path / "replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(my_keeper_path / "dropped", "", zkutil::CreateMode::Persistent));
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

std::optional<StorageKafka2::TopicPartitionLocks>
StorageKafka2::lockTopicPartitions(zkutil::ZooKeeper & keeper_to_use, const TopicPartitions & topic_partitions)
{
    std::vector<fs::path> topic_partition_paths;
    topic_partition_paths.reserve(topic_partitions.size());
    for (const auto & topic_partition : topic_partitions)
        topic_partition_paths.emplace_back(getTopicPartitionPath(topic_partition));

    Coordination::Requests ops;

    static constexpr auto ignore_if_exists = true;

    for (const auto & topic_partition_path : topic_partition_paths)
    {
        const auto lock_file_path = String(topic_partition_path / lock_file_name);
        LOG_TRACE(log, "Creating locking ops for: {}", lock_file_path);
        ops.push_back(zkutil::makeCreateRequest(topic_partition_path, "", zkutil::CreateMode::Persistent, ignore_if_exists));
        ops.push_back(zkutil::makeCreateRequest(lock_file_path, kafka_settings->kafka_replica_name.value, zkutil::CreateMode::Ephemeral));
    }
    Coordination::Responses responses;

    if (const auto code = keeper_to_use.tryMulti(ops, responses); code != Coordination::Error::ZOK)
    {
        if (code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, responses);

        // Possible optimization: check the content of lock files, if we locked them, then we can clean them up and retry to lock them.
        return std::nullopt;
    }

    // We have the locks, let's gather the information we needed
    TopicPartitionLocks locks;
    {
        auto tp_it = topic_partitions.begin();
        auto path_it = topic_partition_paths.begin();
        for (; tp_it != topic_partitions.end(); ++tp_it, ++path_it)
        {
            using zkutil::EphemeralNodeHolder;
            LockedTopicPartitionInfo lock_info{
                EphemeralNodeHolder::existing(*path_it / lock_file_name, keeper_to_use),
                getNumber(keeper_to_use, *path_it / commit_file_name),
                getNumber(keeper_to_use, *path_it / intent_file_name)};

            LOG_TRACE(
                log,
                "Locked topic partition: {}:{} at offset {} with intent size {}",
                tp_it->topic,
                tp_it->partition_id,
                lock_info.committed_offset.value_or(0),
                lock_info.intent_size.value_or(0));
            locks.emplace(TopicPartition(*tp_it), std::move(lock_info));
        }
    }

    return locks;
}


void StorageKafka2::saveCommittedOffset(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition)
{
    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    keeper_to_use.createOrUpdate(partition_prefix / commit_file_name, toString(topic_partition.offset), zkutil::CreateMode::Persistent);
    // This is best effort, if it fails we will try to remove in the next round
    keeper_to_use.tryRemove(partition_prefix / intent_file_name, -1);
    LOG_TEST(
        log, "Saved offset {} for topic-partition [{}:{}]", topic_partition.offset, topic_partition.topic, topic_partition.partition_id);
}

void StorageKafka2::saveIntent(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition, int64_t intent)
{
    LOG_TEST(
        log,
        "Saving intent of {} for topic-partition [{}:{}] at offset {}",
        intent,
        topic_partition.topic,
        topic_partition.partition_id,
        topic_partition.offset);
    keeper_to_use.createOrUpdate(
        getTopicPartitionPath(topic_partition) / intent_file_name, toString(intent), zkutil::CreateMode::Persistent);
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

    auto put_error_to_stream = kafka_settings->kafka_handle_error_mode == StreamingHandleErrorMode::STREAM;

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        getFormatName(), empty_buf, non_virtual_header, modified_context, getMaxBlockSize(), std::nullopt, 1);

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        if (put_error_to_stream)
        {
            exception_message = e.message();
            for (const auto & column : result_columns)
            {
                // read_kafka_message could already push some rows to result_columns
                // before exception, we need to fix it.
                auto cur_rows = column->size();
                if (cur_rows > total_rows)
                    column->popBack(cur_rows - total_rows);

                // all data columns will get default value in case of error
                column->insertDefault();
            }

            return 1;
        }
        else
        {
            e.addMessage(
                "while parsing Kafka message (topic: {}, partition: {}, offset: {})'",
                consumer.currentTopic(),
                consumer.currentPartition(),
                consumer.currentOffset());
            throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));


    Poco::Timespan max_execution_time = kafka_settings->kafka_flush_interval_ms.changed
        ? kafka_settings->kafka_flush_interval_ms
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
            && (total_rows >= getMaxBlockSize() || !check_time_limit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS
                || consumer.needsOffsetUpdate()))
        {
            LOG_TRACE(
                log,
                "Stopped collecting message for current batch. There are {} failed polled attempts, {} total rows and consumer needs "
                "offset update is {}",
                failed_poll_attempts,
                total_rows,
                consumer.needsOffsetUpdate());
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
            && (*maybe_stall_reason == StallReason::KeeperSessionEnded || *maybe_stall_reason == StallReason::CouldNotAcquireLocks))
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
    // In case the initial subscribe in startup failed, let's subscribe now
    consumer->subscribeIfNotSubscribedYet();

    // To keep the consumer alive
    const auto wait_for_assignment = consumer_info.locks.empty();
    LOG_TRACE(log, "Polling consumer {} for events", idx);
    consumer->pollEvents();

    if (wait_for_assignment)
    {
        while (nullptr == consumer->getKafkaAssignment() && consumer_info.watch.elapsedMilliseconds() < MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
            consumer->pollEvents();
        LOG_INFO(log, "Consumer has assignment: {}", nullptr == consumer->getKafkaAssignment());
    }

    try
    {
        if (consumer->needsOffsetUpdate() || consumer_info.locks.empty())
        {
            LOG_TRACE(log, "Consumer needs update offset");
            // First release the locks so let other consumers acquire them ASAP
            consumer_info.locks.clear();
            consumer_info.topic_partitions.clear();

            const auto * current_assignment = consumer->getKafkaAssignment();
            if (current_assignment == nullptr)
            {
                // The consumer lost its assignment and haven't received a new one.
                // By returning true this function reports the current consumer as a "stalled" stream, which
                LOG_TRACE(log, "No assignment");
                return StallReason::NoAssignment;
            }
            consumer_info.consume_from_topic_partition_index = 0;

            if (consumer_info.keeper->expired())
            {
                consumer_info.keeper = getZooKeeperAndAssertActive();
                LOG_TEST(log, "Got new zookeeper");
            }

            auto maybe_locks = lockTopicPartitions(*consumer_info.keeper, *current_assignment);

            if (!maybe_locks.has_value())
            {
                // We couldn't acquire locks, probably some other consumers are still holding them.
                LOG_TRACE(log, "Couldn't acquire locks");
                return StallReason::CouldNotAcquireLocks;
            }

            consumer_info.locks = std::move(*maybe_locks);

            consumer_info.topic_partitions.reserve(current_assignment->size());
            for (const auto & topic_partition : *current_assignment)
            {
                TopicPartition topic_partition_copy{topic_partition};
                if (const auto & maybe_committed_offset = consumer_info.locks.at(topic_partition).committed_offset;
                    maybe_committed_offset.has_value())
                {
                    topic_partition_copy.offset = *maybe_committed_offset;
                }
                // in case no saved offset, we will get the offset from Kafka as a best effort. This is important to not to duplicate message when recreating the table.

                consumer_info.topic_partitions.push_back(std::move(topic_partition_copy));
            }
            consumer_info.consumer->updateOffsets(consumer_info.topic_partitions);
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
            consumer_info.locks.clear();
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
    auto [blocks, last_read_offset] = pollConsumer(
        *consumer_info.consumer, topic_partition, consumer_info.locks[topic_partition].intent_size, consumer_info.watch, kafka_context);

    if (blocks.empty())
    {
        LOG_TRACE(log, "Didn't get any messages");
        needs_offset_reset = false;
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

    auto & keeper_to_use = *consumer_info.keeper;
    auto & lock_info = consumer_info.locks.at(topic_partition);
    lock_info.intent_size = last_read_offset - lock_info.committed_offset.value_or(0);
    saveIntent(keeper_to_use, topic_partition, *lock_info.intent_size);
    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(Pipe{std::make_shared<BlocksListSource>(std::move(blocks))});

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    lock_info.committed_offset = last_read_offset + 1;
    topic_partition.offset = last_read_offset + 1;
    saveCommittedOffset(keeper_to_use, topic_partition);
    consumer_info.consumer->commit(topic_partition);
    lock_info.intent_size.reset();
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
    return fs::path(keeper_path) / "topics" / topic_partition.topic / "partitions" / std::to_string(topic_partition.partition_id);
}

}
