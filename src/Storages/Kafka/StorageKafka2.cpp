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
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include "Storages/Kafka/KafkaConsumer2.h"
#include "config_version.h"

#if USE_KRB5
#    include <Access/KerberosInit.h>
#endif // USE_KRB5

#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <librdkafka/rdkafka.h>

#include <string>

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
extern const Event KafkaMessagesRead;
extern const Event KafkaMessagesFailed;
extern const Event KafkaRowsRead;
extern const Event KafkaRowsRejected;
extern const Event KafkaWrites;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    constexpr auto RESCHEDULE_MS = 500;
    // const auto CLEANUP_TIMEOUT_MS = 3000;
    constexpr auto MAX_THREAD_WORK_DURATION_MS = 60000; // once per minute leave do reschedule (we can't lock threads in pool forever)

    constexpr auto MAX_FAILED_POLL_ATTEMPTS = 10;
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
    , log(&Poco::Logger::get("StorageKafka (" + table_id_.table_name + ")"))
    , semaphore(0, static_cast<int>(num_consumers))
    , intermediate_commit(kafka_settings->kafka_commit_every_batch.value)
    , settings_adjustments(createSettingsAdjustments())
    , thread_per_consumer(kafka_settings->kafka_thread_per_consumer.value)
    , collection_name(collection_name_)
{
    if (kafka_settings->kafka_num_consumers != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Multiple consumers not yet implemented!");

    if (thread_per_consumer)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The new Kafka storage cannot use multiple threads yet!");

    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        kafka_settings->input_format_allow_errors_num = 0;
        kafka_settings->input_format_allow_errors_ratio = 0;
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i] { threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }
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
    {
        kafka_settings->input_format_allow_errors_ratio = 0.;
    }

    if (!kafka_settings->input_format_allow_errors_num.changed)
    {
        kafka_settings->input_format_allow_errors_num = kafka_settings->kafka_skip_broken_messages.value;
    }

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
    {
        boost::trim(topic);
    }
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

    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", brokers);
    conf.set("client.id", client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    // TODO: fill required settings
    updateConfiguration(conf);

    const Settings & settings = getContext()->getSettingsRef();
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();
    const auto & header = metadata_snapshot->getSampleBlockNonMaterialized();

    auto producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), topics[0], std::chrono::milliseconds(poll_timeout), shutdown_called, header);

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
            consumers.push_back(ConsumerAndAssignmentInfo{.consumer = createConsumer(i)});
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }

    createKeeperNodes(consumers.front().consumer);
    // Start the reader thread
    for (auto & task : tasks)
    {
        task->holder->activateAndSchedule();
    }
}


void StorageKafka2::shutdown()
{
    for (auto & task : tasks)
    {
        // Interrupt streaming thread
        task->stream_cancelled = true;

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
    }

    LOG_TRACE(log, "Closing consumers");
    consumers.clear();
    LOG_TRACE(log, "Consumers closed");
}

void StorageKafka2::drop()
{
    getZooKeeper().removeRecursive(kafka_settings->kafka_keeper_path);
}

KafkaConsumer2Ptr StorageKafka2::createConsumer(size_t consumer_number)
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    if (num_consumers > 1)
    {
        conf.set("client.id", fmt::format("{}-{}", client_id, consumer_number));
    }
    else
    {
        conf.set("client.id", client_id);
    }
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    conf.set("auto.offset.reset", "earliest"); // If no offset stored for this group, read all messages from the start

    // that allows to prevent fast draining of the librdkafka queue during building of single insert block. Improves performance significantly, but may lead to bigger memory consumption.
    size_t default_queued_min_messages = 100000; // we don't want to decrease the default
    conf.set("queued.min.messages", std::max(getMaxBlockSize(), default_queued_min_messages));

    updateConfiguration(conf);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false"); // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false"); // Ignore EOF messages

    // Create a consumer and subscribe to topics
    auto consumer_impl = std::make_shared<cppkafka::Consumer>(conf);
    consumer_impl->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    if (thread_per_consumer)
    {
        // call subscribe;
        auto & stream_cancelled = tasks[consumer_number]->stream_cancelled;
        return std::make_shared<KafkaConsumer2>(
            consumer_impl, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), intermediate_commit, stream_cancelled, topics);
    }

    return std::make_shared<KafkaConsumer2>(
        consumer_impl,
        log,
        getPollMaxBatchSize(),
        getPollTimeoutMillisecond(),
        intermediate_commit,
        tasks.back()->stream_cancelled,
        topics);
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

String StorageKafka2::getConfigPrefix() const
{
    if (!collection_name.empty())
        return "named_collections." + collection_name + "."
            + String{KafkaConfigLoader::CONFIG_KAFKA_TAG}; /// Add one more level to separate librdkafka configuration.
    return String{KafkaConfigLoader::CONFIG_KAFKA_TAG};
}

void StorageKafka2::updateConfiguration(cppkafka::Configuration & kafka_config)
{
    // Update consumer configuration from the configuration. Example:
    //     <kafka>
    //         <retry_backoff_ms>250</retry_backoff_ms>
    //         <fetch_min_bytes>100000</fetch_min_bytes>
    //     </kafka>
    const auto & config = getContext()->getConfigRef();
    auto config_prefix = getConfigPrefix();
    if (config.has(config_prefix))
        KafkaConfigLoader::loadConfig(kafka_config, config, config_prefix);

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

    // Update consumer topic-specific configuration (legacy syntax, retained for compatibility). Example with topic "football":
    //     <kafka_football>
    //         <retry_backoff_ms>250</retry_backoff_ms>
    //         <fetch_min_bytes>100000</fetch_min_bytes>
    //     </kafka_football>
    // The legacy syntax has the problem that periods in topic names (e.g. "sports.football") are not supported because the Poco
    // configuration framework hierarchy is based on periods as level separators. Besides that, per-topic tags at the same level
    // as <kafka> are ugly.
    for (const auto & topic : topics)
    {
        const auto topic_config_key = config_prefix + "_" + topic;
        if (config.has(topic_config_key))
            KafkaConfigLoader::loadConfig(kafka_config, config, topic_config_key);
    }

    // Update consumer topic-specific configuration (new syntax). Example with topics "football" and "baseball":
    //     <kafka>
    //         <kafka_topic>
    //             <name>football</name>
    //             <retry_backoff_ms>250</retry_backoff_ms>
    //             <fetch_min_bytes>5000</fetch_min_bytes>
    //         </kafka_topic>
    //         <kafka_topic>
    //             <name>baseball</name>
    //             <retry_backoff_ms>300</retry_backoff_ms>
    //             <fetch_min_bytes>2000</fetch_min_bytes>
    //         </kafka_topic>
    //     </kafka>
    // Advantages: The period restriction no longer applies (e.g. <name>sports.football</name> will work), everything
    // Kafka-related is below <kafka>.
    for (const auto & topic : topics)
        if (config.has(config_prefix))
            KafkaConfigLoader::loadTopicConfig(kafka_config, config, config_prefix, topic);

    // No need to add any prefix, messages can be distinguished
    kafka_config.set_log_callback(
        [this](cppkafka::KafkaHandleBase &, int /*level*/, const std::string & facility, const std::string & message)
        {
            auto [poco_level, client_logs_level] = parseSyslogLevel(1);
            LOG_IMPL(log, client_logs_level, poco_level, "[rdk:{}] {}", facility, message);
        });

    // Configure interceptor to change thread name
    //
    // TODO: add interceptors support into the cppkafka.
    // XXX:  rdkafka uses pthread_set_name_np(), but glibc-compatiblity overrides it to noop.
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

    std::optional<int64_t> getNumber(zkutil::ZooKeeper & keeper, const std::string & path)
    {
        std::string result;
        if (!keeper.tryGet(path, result))
            return std::nullopt;

        return DB::parse<int64_t>(result);
    }
}

void StorageKafka2::createKeeperNodes(const KafkaConsumer2Ptr & consumer)
{
    // TODO(antaljanosbenjamin): check config with other StorageKafkas
    const auto & keeper_path = kafka_settings->kafka_keeper_path.value;

    auto & keeper_ref = getZooKeeper();

    if (keeper_ref.exists(keeper_path))
    {
        return;
    }

    keeper_ref.createAncestors(keeper_path);
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(keeper_path, "", zkutil::CreateMode::Persistent));

    ops.emplace_back(zkutil::makeCreateRequest(keeper_path + "/topics", "", zkutil::CreateMode::Persistent));

    const auto topics_prefix = keeper_path + "/topics/";

    const auto topic_partition_counts = consumer->getPartitionCounts();
    for (const auto & topic_partition_count : topic_partition_counts)
    {
        ops.emplace_back(zkutil::makeCreateRequest(topics_prefix + topic_partition_count.topic, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(
            zkutil::makeCreateRequest(topics_prefix + topic_partition_count.topic + "/partitions", "", zkutil::CreateMode::Persistent));
        const auto partitions_prefix = topics_prefix + topic_partition_count.topic + "/partitions/";
        // TODO(antaljanosbenjamin): handle changing number of partitions
        for (auto partition_id{0U}; partition_id < topic_partition_count.partition_count; ++partition_id)
            ops.emplace_back(zkutil::makeCreateRequest(partitions_prefix + toString(partition_id), "", zkutil::CreateMode::Persistent));
    }


    Coordination::Responses responses;
    const auto code = keeper_ref.tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
}

std::optional<StorageKafka2::TopicPartitionLocks> StorageKafka2::lockTopicPartitions(const TopicPartitions & topic_partitions)
{
    // TODO(antaljanosbenjamin): Review this function with somebody who know keeper better than me
    const auto uuid_as_string = toString(uuid);

    std::vector<std::string> topic_partition_paths;
    topic_partition_paths.reserve(topic_partitions.size());
    for (const auto & topic_partition : topic_partitions)
    {
        topic_partition_paths.emplace_back(getTopicPartitionPath(topic_partition));
    }

    Coordination::Requests ops;

    // for (const auto & topic_partition_path : topic_partition_paths)
    //     ops.push_back(zkutil::makeCheckRequest(topic_partition_path + lock_file_name, -1));

    for (const auto & topic_partition_path : topic_partition_paths)
        ops.push_back(zkutil::makeCreateRequest(topic_partition_path + lock_file_name, uuid_as_string, zkutil::CreateMode::Ephemeral));

    bool success = false;
    for (auto try_count{0}; try_count < 10; ++try_count)
    {
        Coordination::Responses responses;
        // TODO(antaljanosbenjamin): this can go wrong if we start a new session simultaneously from multiple threads.
        auto & keeper_ref = getZooKeeper();

        if (const auto code = keeper_ref.tryMulti(ops, responses); code == Coordination::Error::ZOK)
        {
            success = true;
            break;
        }
        else
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        // TODO(antaljanosbenjamin): Probably handle the most common errors
        for (const auto & topic_partition_path : topic_partition_paths)
            keeper_ref.waitForDisappear(topic_partition_path + lock_file_name);
    }

    if (!success)
        return std::nullopt;


    // We have the locks
    TopicPartitionLocks locks;
    {
        auto & keeper_ref = getZooKeeper();
        auto tp_it = topic_partitions.begin();
        auto path_it = topic_partition_paths.begin();
        for (; tp_it != topic_partitions.end(); ++tp_it, ++path_it)
        {
            using zkutil::EphemeralNodeHolder;
            LockedTopicPartitionInfo lock_info{.lock = EphemeralNodeHolder::existing(*path_it + lock_file_name, keeper_ref)};

            lock_info.committed_offset = getNumber(keeper_ref, *path_it + commit_file_name);
            lock_info.intent_size = getNumber(keeper_ref, *path_it + intent_file_name);


            locks.emplace(TopicPartition(*tp_it), std::move(lock_info));
        }
    }

    return locks;
}


void StorageKafka2::saveCommittedOffset(const TopicPartition & topic_partition, int64_t committed_offset)
{
    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    auto & keeper_ref = getZooKeeper();
    keeper_ref.createOrUpdate(partition_prefix + commit_file_name, toString(committed_offset), zkutil::CreateMode::Persistent);
    // This is best effort, if it fails we will try to remove in the next round
    keeper_ref.tryRemove(partition_prefix + intent_file_name, -1);
}

void StorageKafka2::saveIntent(const TopicPartition & topic_partition, int64_t intent)
{
    getZooKeeper().createOrUpdate(
        getTopicPartitionPath(topic_partition) + intent_file_name, toString(intent), zkutil::CreateMode::Persistent);
}


StorageKafka2::PolledBatchInfo
StorageKafka2::pollConsumer(KafkaConsumer2 & consumer, const TopicPartition & topic_partition, const ContextPtr & modified_context)
{
    PolledBatchInfo batch_info;
    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    Block non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized());
    Block virtual_header(storage_snapshot->getSampleBlockForColumns(getVirtualColumnNames()));

    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto put_error_to_stream = kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM;

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

    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

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
        if (auto buf = consumer.consume(topic_partition))
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
        // TODO(antaljanosbenjamin): think about this when rebalance is happening, because `isStalled()` will return true
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
            && (total_rows >= kafka_settings->kafka_max_block_size || !check_time_limit()
                || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS || consumer.needsOffsetUpdate()))
        {
            break;
        }
    }

    if (total_rows == 0)
    {
        return {};
    }

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
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
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
        task->holder->scheduleAfter(RESCHEDULE_MS);
}

bool StorageKafka2::streamToViews(size_t idx)
{
    // What to do?
    // 1. Select a topic partition to consume from
    // 2. Do a casual poll for every other consumer to keep them alive
    // 3. Get the necessary data from Keeper
    // 4. Get the corresponding consumer
    // 5. Pull messages
    // 6. Create a BlockList from it
    // 7. Execute the pipeline
    // 8. Write the offset to Keeper

    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaBackgroundReads};
    ProfileEvents::increment(ProfileEvents::KafkaBackgroundReads);

    auto & consumer_info = consumers[idx];
    auto & consumer = consumer_info.consumer;
    // To keep the consumer alive

    LOG_TRACE(log, "Polling consumer #{} for events", idx);
    consumer->pollEvents();
    if (nullptr == consumer->getKafkaAssignment())
        return true;


    LOG_TRACE(log, "Consumer #{} has assignment", idx);

    if (consumer->needsOffsetUpdate())
    {
        LOG_TRACE(log, "Consumer #{} needs update offset", idx);
        consumer_info.consume_from_topic_partition_index = 0;

        consumer_info.locks.clear();
        consumer_info.topic_partitions.clear();

        if (const auto * current_assignment = consumer->getKafkaAssignment(); nullptr != current_assignment)
        {
            auto maybe_locks = lockTopicPartitions(*current_assignment);

            if (!maybe_locks.has_value())
            {
                // TODO(antaljanosbenjamin): signal this somehow to caller, maybe wait a bit longer.
                return true;
            }

            consumer_info.locks = std::move(*maybe_locks);

            consumer_info.topic_partitions.reserve(current_assignment->size());
            for (const auto & topic_partition : *current_assignment)
            {
                TopicPartition topic_partition_copy{topic_partition};
                if (const auto & maybe_committed_offset = consumer_info.locks.at(topic_partition).committed_offset;
                    maybe_committed_offset.has_value())
                    topic_partition_copy.offset = *maybe_committed_offset + 1;
                else
                    topic_partition_copy.offset = KafkaConsumer2::BEGINNING_OFFSET;
                consumer_info.topic_partitions.push_back(std::move(topic_partition_copy));
            }
        }
        consumer_info.consumer->updateOffsets(consumer_info.topic_partitions);
    }

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

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
        "Consumer #{} will fetch {}:{} (consume_from_topic_partition_index is {})",
        idx,
        topic_partition.topic,
        topic_partition.partition_id,
        consumer_info.consume_from_topic_partition_index);
    consumer_info.consume_from_topic_partition_index
        = (consumer_info.consume_from_topic_partition_index + 1) % consumer_info.topic_partitions.size();

    auto [blocks, last_offset] = pollConsumer(*consumer_info.consumer, topic_partition, kafka_context);

    if (blocks.empty())
    {
        LOG_TRACE(log, "Consumer #{} didn't get any messages", idx);
        return true;
    }


    auto converting_dag = ActionsDAG::makeConvertingActions(
        blocks.front().cloneEmpty().getColumnsWithTypeAndName(),
        block_io.pipeline.getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));

    for (auto & block : blocks)
    {
        converting_actions->execute(block);
    }

    // We can't cancel during copyData, as it's not aware of commits and other kafka-related stuff.
    // It will be cancelled on underlying layer (kafka buffer)

    auto & lock_info = consumer_info.locks.at(topic_partition);
    const auto intent = lock_info.committed_offset.value_or(0);
    saveIntent(topic_partition, intent);
    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(Pipe{std::make_shared<BlocksListSource>(std::move(blocks))});

        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    saveCommittedOffset(topic_partition, last_offset);
    lock_info.intent_size = intent;
    lock_info.committed_offset = last_offset;
    topic_partition.offset = last_offset;

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.", formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return false;
}


zkutil::ZooKeeper & StorageKafka2::getZooKeeper()
{
    if (keeper->expired())
    {
        keeper = keeper->startNewSession();
        //TODO(antaljanosbenjamin): handle ephemeral nodes
    }
    return *keeper;
}


std::string StorageKafka2::getTopicPartitionPath(const TopicPartition & topic_partition)
{
    return kafka_settings->kafka_keeper_path.value + "/topics/" + topic_partition.topic + "/partitions/"
        + std::to_string(topic_partition.partition_id) + '/';
}

NamesAndTypesList StorageKafka2::getVirtuals() const
{
    auto result = NamesAndTypesList{
        {"_topic", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_key", std::make_shared<DataTypeString>()},
        {"_offset", std::make_shared<DataTypeUInt64>()},
        {"_partition", std::make_shared<DataTypeUInt64>()},
        {"_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3))},
        {"_headers.name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"_headers.value", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}};
    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        result.push_back({"_raw_message", std::make_shared<DataTypeString>()});
        result.push_back({"_error", std::make_shared<DataTypeString>()});
    }
    return result;
}

Names StorageKafka2::getVirtualColumnNames() const
{
    auto result = Names{
        "_topic",
        "_key",
        "_offset",
        "_partition",
        "_timestamp",
        "_timestamp_ms",
        "_headers.name",
        "_headers.value",
    };
    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        result.push_back({"_raw_message"});
        result.push_back({"_error"});
    }
    return result;
}

}
