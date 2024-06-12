#include <Storages/Kafka/StorageKafka2.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
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
#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafkaCommon.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <base/getFQDNOrHostName.h>
#include <base/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "Common/config_version.h"
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include "Storages/Kafka/KafkaConsumer2.h"

#if USE_KRB5
#    include <Access/KerberosInit.h>
#endif // USE_KRB5

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

namespace fs = std::filesystem;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int REPLICA_ALREADY_EXISTS;
extern const int TABLE_IS_DROPPED;
extern const int TABLE_WAS_NOT_DROPPED;
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
    std::unique_ptr<KafkaSettings> kafka_settings_,
    const String & collection_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , keeper(getContext()->getZooKeeper())
    , kafka_settings(std::move(kafka_settings_))
    , macros_info{.table_id = table_id_}
    , topics(parseTopics(getContext()->getMacros()->expand(kafka_settings->kafka_topic_list.value, macros_info)))
    , brokers(getContext()->getMacros()->expand(kafka_settings->kafka_broker_list.value, macros_info))
    , group(getContext()->getMacros()->expand(kafka_settings->kafka_group_name.value, macros_info))
    , client_id(
          kafka_settings->kafka_client_id.value.empty()
              ? getDefaultClientId(table_id_)
              : getContext()->getMacros()->expand(kafka_settings->kafka_client_id.value, macros_info))
    , format_name(getContext()->getMacros()->expand(kafka_settings->kafka_format.value))
    , max_rows_per_message(kafka_settings->kafka_max_rows_per_message.value)
    , schema_name(getContext()->getMacros()->expand(kafka_settings->kafka_schema.value, macros_info))
    , num_consumers(kafka_settings->kafka_num_consumers.value)
    , log(getLogger("StorageKafka2 (" + table_id_.table_name + ")"))
    , semaphore(0, static_cast<int>(num_consumers))
    , settings_adjustments(createSettingsAdjustments())
    , thread_per_consumer(kafka_settings->kafka_thread_per_consumer.value)
    , collection_name(collection_name_)
{
    if (kafka_settings->kafka_num_consumers > 1 && !thread_per_consumer)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "With multiple consumer you have to use thread per consumer!");

    if (kafka_settings->kafka_handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        kafka_settings->input_format_allow_errors_num = 0;
        kafka_settings->input_format_allow_errors_ratio = 0;
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(createVirtuals(kafka_settings->kafka_handle_error_mode));

    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i] { threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            consumers.push_back(ConsumerAndAssignmentInfo{.consumer = createConsumer(i), .keeper = keeper});
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }

    const auto first_replica = createTableIfNotExists();

    if (!first_replica)
        createReplica();
}

VirtualColumnsDescription StorageKafka2::createVirtuals(StreamingHandleErrorMode handle_error_mode)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_topic", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    desc.addEphemeral("_key", std::make_shared<DataTypeString>(), "");
    desc.addEphemeral("_offset", std::make_shared<DataTypeUInt64>(), "");
    desc.addEphemeral("_partition", std::make_shared<DataTypeUInt64>(), "");
    desc.addEphemeral("_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "");
    desc.addEphemeral("_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3)), "");
    desc.addEphemeral("_headers.name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "");
    desc.addEphemeral("_headers.value", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "");

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeString>(), "");
        desc.addEphemeral("_error", std::make_shared<DataTypeString>(), "");
    }

    return desc;
}

SettingsChanges StorageKafka2::createSettingsAdjustments()
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (!kafka_settings->input_format_skip_unknown_fields.changed)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        kafka_settings->input_format_skip_unknown_fields = true;
    }

    if (!kafka_settings->input_format_allow_errors_ratio.changed)
        kafka_settings->input_format_allow_errors_ratio = 0.;

    if (!kafka_settings->input_format_allow_errors_num.changed)
        kafka_settings->input_format_allow_errors_num = kafka_settings->kafka_skip_broken_messages.value;

    if (!schema_name.empty())
        result.emplace_back("format_schema", schema_name);

    for (const auto & setting : *kafka_settings)
    {
        const auto & name = setting.getName();
        if (name.find("kafka_") == std::string::npos)
            result.emplace_back(name, setting.getValue());
    }
    return result;
}

Names StorageKafka2::parseTopics(String topic_list)
{
    Names result;
    boost::split(result, topic_list, [](char c) { return c == ','; });
    for (String & topic : result)
        boost::trim(topic);
    return result;
}

String StorageKafka2::getDefaultClientId(const StorageID & table_id_)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id_.database_name, table_id_.table_name);
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
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "You cannot read from the new Kafka storage!");
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
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();
    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), topics[0], std::chrono::milliseconds(poll_timeout), shutdown_called, header);

    LOG_TRACE(log, "Kafka producer created");

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef().output_format_avro_rows_in_file.changed)
        max_rows = local_context->getSettingsRef().output_format_avro_rows_in_file.value;
    return std::make_shared<MessageQueueSink>(header, getFormatName(), max_rows, std::move(producer), getName(), modified_context);
}

void StorageKafka2::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            consumers.emplace_back(ConsumerAndAssignmentInfo{.consumer = createConsumer(i), .keeper = keeper});
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }
    // Start the reader thread
    for (auto & task : tasks)
        task->holder->activateAndSchedule();
}


void StorageKafka2::shutdown(bool)
{
    shutdown_called = true;
    for (auto & task : tasks)
    {
        LOG_TRACE(log, "Cancelling streams");
        // Interrupt streaming thread
        task->stream_cancelled = true;
    }

    for (auto & task : tasks)
    {
        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
    }

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
    if (thread_per_consumer)
    {
        // call subscribe;
        auto & stream_cancelled = tasks[consumer_number]->stream_cancelled;
        return std::make_shared<KafkaConsumer2>(
            consumer_impl, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), stream_cancelled, topics);
    }

    return std::make_shared<KafkaConsumer2>(
        consumer_impl, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), tasks.back()->stream_cancelled, topics);
}


cppkafka::Configuration StorageKafka2::getConsumerConfiguration(size_t consumer_number)
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    if (num_consumers > 1)
        conf.set("client.id", fmt::format("{}-{}", client_id, consumer_number));
    else
        conf.set("client.id", client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    conf.set("auto.offset.reset", "earliest"); // If no offset stored for this group, read all messages from the start

    // that allows to prevent fast draining of the librdkafka queue
    // during building of single insert block. Improves performance
    // significantly, but may lead to bigger memory consumption.
    size_t default_queued_min_messages = 100000; // must be greater than or equal to default
    size_t max_allowed_queued_min_messages = 10000000; // must be less than or equal to max allowed value
    conf.set("queued.min.messages", std::min(std::max(getMaxBlockSize(), default_queued_min_messages), max_allowed_queued_min_messages));

    updateGlobalConfiguration(conf);
    updateConsumerConfiguration(conf);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false"); // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false"); // Ignore EOF messages

    for (auto & property : conf.get_all())
        LOG_TRACE(log, "Consumer set property {}:{}", property.first, property.second);

    return conf;
}

cppkafka::Configuration StorageKafka2::getProducerConfiguration()
{
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", brokers);
    conf.set("client.id", client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);

    updateGlobalConfiguration(conf);
    updateProducerConfiguration(conf);

    for (auto & property : conf.get_all())
        LOG_TRACE(log, "Producer set property {}:{}", property.first, property.second);

    return conf;
}

size_t StorageKafka2::getMaxBlockSize() const
{
    return kafka_settings->kafka_max_block_size.changed ? kafka_settings->kafka_max_block_size.value
                                                        : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}

size_t StorageKafka2::getPollMaxBatchSize() const
{
    size_t batch_size = kafka_settings->kafka_poll_max_batch_size.changed ? kafka_settings->kafka_poll_max_batch_size.value
                                                                          : getContext()->getSettingsRef().max_block_size.value;

    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageKafka2::getPollTimeoutMillisecond() const
{
    return kafka_settings->kafka_poll_timeout_ms.changed ? kafka_settings->kafka_poll_timeout_ms.totalMilliseconds()
                                                         : getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}

void StorageKafka2::updateGlobalConfiguration(cppkafka::Configuration & kafka_config)
{
    const auto & config = getContext()->getConfigRef();
    KafkaConfigLoader::loadFromConfig(kafka_config, config, collection_name, KafkaConfigLoader::CONFIG_KAFKA_TAG, topics);

#if USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.kinit.cmd"))
        LOG_WARNING(log, "sasl.kerberos.kinit.cmd configuration parameter is ignored.");

    kafka_config.set("sasl.kerberos.kinit.cmd", "");
    kafka_config.set("sasl.kerberos.min.time.before.relogin", "0");

    if (kafka_config.has_property("sasl.kerberos.keytab") && kafka_config.has_property("sasl.kerberos.principal"))
    {
        String keytab = kafka_config.get("sasl.kerberos.keytab");
        String principal = kafka_config.get("sasl.kerberos.principal");
        LOG_DEBUG(log, "Running KerberosInit");
        try
        {
            kerberosInit(keytab, principal);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "KerberosInit failure: {}", getExceptionMessage(e, false));
        }
        LOG_DEBUG(log, "Finished KerberosInit");
    }
#else // USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.keytab") || kafka_config.has_property("sasl.kerberos.principal"))
        LOG_WARNING(log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
#endif // USE_KRB5
    // No need to add any prefix, messages can be distinguished
    kafka_config.set_log_callback(
        [this](cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message)
        {
            auto [poco_level, client_logs_level] = parseSyslogLevel(level);
            const auto & kafka_object_config = handle.get_configuration();
            const std::string client_id_key{"client.id"};
            chassert(kafka_object_config.has_property(client_id_key) && "Kafka configuration doesn't have expected client.id set");
            LOG_IMPL(
                log,
                client_logs_level,
                poco_level,
                "[client.id:{}] [rdk:{}] {}",
                kafka_object_config.get(client_id_key),
                facility,
                message);
        });

    /// NOTE: statistics should be consumed, otherwise it creates too much
    /// entries in the queue, that leads to memory leak and slow shutdown.
    if (!kafka_config.has_property("statistics.interval.ms"))
    {
        // every 3 seconds by default. set to 0 to disable.
        kafka_config.set("statistics.interval.ms", "3000");
    }

    // Configure interceptor to change thread name
    //
    // TODO: add interceptors support into the cppkafka.
    // XXX:  rdkafka uses pthread_set_name_np(), but glibc-compatibility overrides it to noop.
    {
        // This should be safe, since we wait the rdkafka object anyway.
        void * self = static_cast<void *>(this);

        int status;

        status = rd_kafka_conf_interceptor_add_on_new(kafka_config.get_handle(), "init", StorageKafkaInterceptors::rdKafkaOnNew, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set new interceptor due to {} error", status);

        // cppkafka always copy the configuration
        status = rd_kafka_conf_interceptor_add_on_conf_dup(
            kafka_config.get_handle(), "init", StorageKafkaInterceptors::rdKafkaOnConfDup, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set dup conf interceptor due to {} error", status);
    }
}

void StorageKafka2::updateConsumerConfiguration(cppkafka::Configuration & kafka_config)
{
    const auto & config = getContext()->getConfigRef();
    KafkaConfigLoader::loadConsumerConfig(kafka_config, config, collection_name, KafkaConfigLoader::CONFIG_KAFKA_TAG, topics);
}

void StorageKafka2::updateProducerConfiguration(cppkafka::Configuration & kafka_config)
{
    const auto & config = getContext()->getConfigRef();
    KafkaConfigLoader::loadProducerConfig(kafka_config, config, collection_name, KafkaConfigLoader::CONFIG_KAFKA_TAG, topics);
}

String StorageKafka2::getConfigPrefix() const
{
    if (!collection_name.empty())
        return "named_collections." + collection_name + "."
            + String{KafkaConfigLoader::CONFIG_KAFKA_TAG}; /// Add one more level to separate librdkafka configuration.
    return String{KafkaConfigLoader::CONFIG_KAFKA_TAG};
}

bool StorageKafka2::checkDependencies(const StorageID & table_id)
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
    const auto & keeper_path = fs::path(kafka_settings->kafka_keeper_path.value);

    const auto & replicas_path = keeper_path / "replicas";

    for (auto i = 0; i < 1000; ++i)
    {
        if (keeper->exists(replicas_path))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", String(keeper_path));
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (keeper->exists(keeper_path / "dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {} (this might take several minutes)", String(keeper_path));
            String drop_lock_path = keeper_path / "dropped" / "lock";
            Coordination::Error code = keeper->tryCreate(drop_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "The leftovers from table {} were removed by another replica", String(keeper_path));
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception::fromPath(code, drop_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *keeper);
                if (!removeTableNodesFromZooKeeper(metadata_drop_lock))
                {
                    /// Someone is recursively removing table right now, we cannot create new table until old one is removed
                    continue;
                }
            }
        }

        keeper->createAncestors(keeper_path);
        Coordination::Requests ops;

        ops.emplace_back(zkutil::makeCreateRequest(keeper_path, "", zkutil::CreateMode::Persistent));

        const auto topics_path = keeper_path / "topics";
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
        ops.emplace_back(
            zkutil::makeCreateRequest(replicas_path / kafka_settings->kafka_replica_name.value, "", zkutil::CreateMode::Persistent));


        Coordination::Responses responses;
        const auto code = keeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another replica at the same moment, will retry", String(keeper_path));
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        LOG_INFO(log, "Table {} created successfully ", String(keeper_path));

        return true;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper_path or because of logical error");
}


bool StorageKafka2::removeTableNodesFromZooKeeper(const zkutil::EphemeralNodeHolder::Ptr & drop_lock)
{
    bool completely_removed = false;

    Strings children;
    if (const auto code = keeper->tryGetChildren(kafka_settings->kafka_keeper_path.value, children); code == Coordination::Error::ZNONODE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal. It's a bug");

    const auto keeper_path = fs::path(kafka_settings->kafka_keeper_path.value);
    for (const auto & child : children)
        if (child != "dropped")
            keeper->tryRemoveRecursive(keeper_path / child);

    Coordination::Requests ops;
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(keeper_path / "dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(keeper_path, -1));
    const auto code = keeper->tryMulti(ops, responses, /* check_session_valid */ true);

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
            kafka_settings->kafka_keeper_path.value);
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
        LOG_INFO(log, "Table {} was successfully removed from ZooKeeper", kafka_settings->kafka_keeper_path.value);
    }

    return completely_removed;
}

void StorageKafka2::createReplica()
{
    const auto replica_path = kafka_settings->kafka_keeper_path.value + "/replicas/" + kafka_settings->kafka_replica_name.value;
    const auto code = keeper->tryCreate(replica_path, "", zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZNODEEXISTS)
        throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS, "Replica {} already exists", replica_path);
    else if (code == Coordination::Error::ZNONODE)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} was suddenly removed", kafka_settings->kafka_keeper_path.value);
    else if (code != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(code, replica_path);

    LOG_INFO(log, "Replica {} created", replica_path);
}


void StorageKafka2::dropReplica()
{
    if (keeper->expired())
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Table was not dropped because ZooKeeper session has expired.");

    auto replica_path = kafka_settings->kafka_keeper_path.value + "/replicas/" + kafka_settings->kafka_replica_name.value;

    LOG_INFO(log, "Removing replica {}", replica_path);

    if (!keeper->exists(replica_path))
    {
        LOG_INFO(log, "Removing replica {} does not exist", replica_path);
        return;
    }

    {
        keeper->tryRemoveChildrenRecursive(replica_path);

        if (keeper->tryRemove(replica_path) != Coordination::Error::ZOK)
            LOG_ERROR(log, "Replica was not completely removed from Keeper, {} still exists and may contain some garbage.", replica_path);
    }

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (Coordination::Error::ZOK != keeper->tryGetChildren(kafka_settings->kafka_keeper_path.value + "/replicas", replicas)
        || !replicas.empty())
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
    String drop_lock_path = kafka_settings->kafka_keeper_path.value + "/dropped/lock";
    ops.emplace_back(zkutil::makeRemoveRequest(kafka_settings->kafka_keeper_path.value + "/replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(kafka_settings->kafka_keeper_path.value + "/dropped", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
    Coordination::Error code = keeper->tryMulti(ops, responses);

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
        auto drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *keeper);
        LOG_INFO(log, "Removing table {} (this might take several minutes)", kafka_settings->kafka_keeper_path.value);
        removeTableNodesFromZooKeeper(drop_lock);
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

        // Possible optimization: check the content of logfiles, if we locked them, then we can clean them up and retry to lock them.
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
            LockedTopicPartitionInfo lock_info{.lock = EphemeralNodeHolder::existing(*path_it / lock_file_name, keeper_to_use)};

            lock_info.committed_offset = getNumber(keeper_to_use, *path_it / commit_file_name);
            lock_info.intent_size = getNumber(keeper_to_use, *path_it / intent_file_name);

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
        : getContext()->getSettingsRef().stream_flush_interval_ms;

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
    // i.e. will not be stored anythere
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
    assert(idx < tasks.size());
    auto task = tasks[idx];
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
            while (!task->stream_cancelled && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

                // Exit the loop & reschedule if some stream stalled
                auto some_stream_is_stalled = streamToViews(idx);
                if (some_stream_is_stalled)
                {
                    LOG_TRACE(log, "Stream(s) stalled. Reschedule.");
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

    mv_attached.store(false);

    // Wait for attached views
    if (!task->stream_cancelled)
        task->holder->scheduleAfter(KAFKA_RESCHEDULE_MS);
}

bool StorageKafka2::streamToViews(size_t idx)
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

    // To keep the consumer alive
    const auto wait_for_assignment = consumer_info.locks.empty();
    LOG_TRACE(log, "Polling consumer for events");
    consumer->pollEvents();

    if (wait_for_assignment)
    {
        while (nullptr == consumer->getKafkaAssignment() && consumer_info.watch.elapsedMilliseconds() < MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
            consumer->pollEvents();
    }

    try
    {
        if (consumer->needsOffsetUpdate() || consumer_info.locks.empty())
        {
            // First release the locks so let other consumers acquire them ASAP
            consumer_info.locks.clear();

            const auto * current_assignment = consumer->getKafkaAssignment();
            if (current_assignment == nullptr)
            {
                // The consumer lost its assignment and haven't received a new one.
                // By returning true this function reports the current consumer as a "stalled" stream, which
                LOG_TRACE(log, "No assignment");
                return true;
            }
            LOG_TRACE(log, "Consumer needs update offset");
            consumer_info.consume_from_topic_partition_index = 0;

            consumer_info.locks.clear();
            consumer_info.topic_partitions.clear();

            auto maybe_locks = lockTopicPartitions(*consumer_info.keeper, *current_assignment);

            if (!maybe_locks.has_value())
            {
                // We couldn't acquire locks, probably some other consumers are still holding them.
                LOG_TRACE(log, "Couldn't acquire locks");
                return true;
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
            return true;
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
            return true;
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (Coordination::isHardwareError(e.code))
        {
            // Clear ephemeral nodes here as we got a new keeper here
            consumer_info.locks.clear();
            consumer_info.keeper = getZooKeeper();
            return true;
        }

        throw;
    }
    return false;
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
    InterpreterInsertQuery interpreter(insert, kafka_context, false, true, true);
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
    consumer_info.consumer->commit(topic_partition);
    saveCommittedOffset(keeper_to_use, topic_partition);
    lock_info.intent_size.reset();
    needs_offset_reset = false;

    return rows;
}


zkutil::ZooKeeperPtr StorageKafka2::getZooKeeper()
{
    std::unique_lock lock{keeper_mutex};
    if (keeper->expired())
    {
        keeper = keeper->startNewSession();
    }
    return keeper;
}


fs::path StorageKafka2::getTopicPartitionPath(const TopicPartition & topic_partition)
{
    return fs::path(kafka_settings->kafka_keeper_path.value) / "topics" / topic_partition.topic / "partitions"
        / std::to_string(topic_partition.partition_id);
}

}
