#include <Storages/Kafka/StorageKafkaCommon.h>


#include <Databases/DatabaseReplicatedHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/config_version.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/replace.hpp>
#include <cppkafka/cppkafka.h>
#include <librdkafka/rdkafka.h>

#if USE_KRB5
#    include <Access/KerberosInit.h>
#endif // USE_KRB5

namespace CurrentMetrics
{
extern const Metric KafkaLibrdkafkaThreads;
}

namespace ProfileEvents
{
extern const Event KafkaConsumerErrors;
}

namespace DB
{

using namespace std::chrono_literals;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t
StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx)
{
        TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
        CurrentMetrics::add(CurrentMetrics::KafkaLibrdkafkaThreads, 1);

        const auto & storage_id = self->getStorageID();
        const auto & table = storage_id.getTableName();

        switch (thread_type)
        {
            case RD_KAFKA_THREAD_MAIN:
                setThreadName(("rdk:m/" + table.substr(0, 9)).c_str());
                break;
            case RD_KAFKA_THREAD_BACKGROUND:
                setThreadName(("rdk:bg/" + table.substr(0, 8)).c_str());
                break;
            case RD_KAFKA_THREAD_BROKER:
                setThreadName(("rdk:b/" + table.substr(0, 9)).c_str());
                break;
        }

        /// Create ThreadStatus to track memory allocations from librdkafka threads.
        //
        /// And store them in a separate list (thread_statuses) to make sure that they will be destroyed,
        /// regardless how librdkafka calls the hooks.
        /// But this can trigger use-after-free if librdkafka will not destroy threads after rd_kafka_wait_destroyed()
        auto thread_status = std::make_shared<ThreadStatus>();
        std::lock_guard lock(self->thread_statuses_mutex);
        self->thread_statuses.emplace_back(std::move(thread_status));

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t
StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    CurrentMetrics::sub(CurrentMetrics::KafkaLibrdkafkaThreads, 1);

    std::lock_guard lock(self->thread_statuses_mutex);
    const auto it = std::find_if(self->thread_statuses.begin(), self->thread_statuses.end(), [](const auto & thread_status_ptr)
    {
        return thread_status_ptr.get() == current_thread;
    });
    if (it == self->thread_statuses.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No thread status for this librdkafka thread.");

    self->thread_statuses.erase(it);

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnNew(
    rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    rd_kafka_resp_err_t status;

    status = rd_kafka_interceptor_add_on_thread_start(rk, "init-thread", rdKafkaOnThreadStart, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(self->log, "Cannot set on thread start interceptor due to {} error", status);
        return status;
    }

    status = rd_kafka_interceptor_add_on_thread_exit(rk, "exit-thread", rdKafkaOnThreadExit, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(self->log, "Cannot set on thread exit interceptor due to {} error", status);

    return status;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnConfDup(
    rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    rd_kafka_resp_err_t status;

    // cppkafka copies configuration multiple times
    status = rd_kafka_conf_interceptor_add_on_conf_dup(new_conf, "init", rdKafkaOnConfDup, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(self->log, "Cannot set on conf dup interceptor due to {} error", status);
        return status;
    }

    status = rd_kafka_conf_interceptor_add_on_new(new_conf, "init", rdKafkaOnNew, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(self->log, "Cannot set on conf new interceptor due to {} error", status);

    return status;
}

void setKafkaConfigValue(cppkafka::Configuration & kafka_config, const String & key, const String & value)
{
    /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
    /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    const String setting_name_in_kafka_config = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
    kafka_config.set(setting_name_in_kafka_config, value);
}

void loadConfigProperty(cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const String & tag)
{
    const String property_path = config_prefix + "." + tag;
    const String property_value = config.getString(property_path);

    setKafkaConfigValue(kafka_config, tag, property_value);
}

void loadNamedCollectionConfig(cppkafka::Configuration & kafka_config, const String & collection_name, const String & config_prefix)
{
    const auto & collection = NamedCollectionFactory::instance().get(collection_name);
    for (const auto & key : collection->getKeys(-1, config_prefix))
    {
        // Cut prefix with '.' before actual config tag.
        const auto param_name = key.substr(config_prefix.size() + 1);
        setKafkaConfigValue(kafka_config, param_name, collection->get<String>(key));
    }
}

void loadLegacyTopicConfig(cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & collection_name, const String & config_prefix)
{
    if (!collection_name.empty())
    {
        loadNamedCollectionConfig(kafka_config, collection_name, config_prefix);
        return;
    }

    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        loadConfigProperty(kafka_config, config, config_prefix, tag);
    }
}

/// Read server configuration into cppkafa configuration, used by new per-topic configuration
void loadTopicConfig(cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & collection_name, const String & config_prefix, const String & topic)
{
    if (!collection_name.empty())
    {
        const auto topic_prefix = fmt::format("{}.{}", config_prefix, KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG);
        const auto & collection = NamedCollectionFactory::instance().get(collection_name);
        for (const auto & key : collection->getKeys(1, config_prefix))
        {
            /// Only consider key <kafka_topic>. Multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
            if (!key.starts_with(topic_prefix))
                continue;

            const String kafka_topic_path = config_prefix + "." + key;
            const String kafka_topic_name_path = kafka_topic_path + "." + KafkaConfigLoader::CONFIG_NAME_TAG;
            if (topic == collection->get<String>(kafka_topic_name_path))
                /// Found it! Now read the per-topic configuration into cppkafka.
                loadNamedCollectionConfig(kafka_config, collection_name, kafka_topic_path);
        }
    }
    else
    {
        /// Read all tags one level below <kafka>
        Poco::Util::AbstractConfiguration::Keys tags;
        config.keys(config_prefix, tags);

        for (const auto & tag : tags)
        {
            if (tag == KafkaConfigLoader::CONFIG_NAME_TAG)
                continue; // ignore <name>, it is used to match topic configurations
            loadConfigProperty(kafka_config, config, config_prefix, tag);
        }
    }
}

/// Read server configuration into cppkafka configuration, used by global configuration and by legacy per-topic configuration
static void
loadFromConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix)
{
    if (!params.collection_name.empty())
    {
        loadNamedCollectionConfig(kafka_config, params.collection_name, config_prefix);
        return;
    }

    /// Read all tags one level below <kafka>
    Poco::Util::AbstractConfiguration::Keys tags;
    params.config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        if (tag == KafkaConfigLoader::CONFIG_KAFKA_PRODUCER_TAG || tag == KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG)
            /// Do not load consumer/producer properties, since they should be separated by different configuration objects.
            continue;

        if (tag.starts_with(KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG)) /// multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
        {
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
            for (const auto & topic : params.topics)
            {
                /// Read topic name between <name>...</name>
                const String kafka_topic_path = config_prefix + "." + tag;
                const String kafka_topic_name_path = kafka_topic_path + "." + KafkaConfigLoader::CONFIG_NAME_TAG;
                const String topic_name = params.config.getString(kafka_topic_name_path);

                if (topic_name != topic)
                    continue;
                loadTopicConfig(kafka_config, params.config, params.collection_name, kafka_topic_path, topic);
            }
            continue;
        }
        if (tag.starts_with(KafkaConfigLoader::CONFIG_KAFKA_TAG))
            /// skip legacy configuration per topic e.g. <kafka_TOPIC_NAME>.
            /// it will be processed is a separate function
            continue;
        // Update configuration from the configuration. Example:
        //     <kafka>
        //         <retry_backoff_ms>250</retry_backoff_ms>
        //         <fetch_min_bytes>100000</fetch_min_bytes>
        //     </kafka>
        loadConfigProperty(kafka_config, params.config, config_prefix, tag);
    }
}

void loadLegacyConfigSyntax(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const Names & topics)
{
    for (const auto & topic : topics)
    {
        const String kafka_topic_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_TAG + "_" + topic;
        loadLegacyTopicConfig(kafka_config, config, collection_name, kafka_topic_path);
    }
}

static void loadConsumerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String consumer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, consumer_path);
}

static void loadProducerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String producer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_PRODUCER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, producer_path);
}

template <typename TKafkaStorage>
static void updateGlobalConfiguration(
    cppkafka::Configuration & kafka_config, TKafkaStorage & storage, const KafkaConfigLoader::LoadConfigParams & params)
{
    loadFromConfig(kafka_config, params, KafkaConfigLoader::CONFIG_KAFKA_TAG);

#if USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.kinit.cmd"))
        LOG_WARNING(params.log, "sasl.kerberos.kinit.cmd configuration parameter is ignored.");

    kafka_config.set("sasl.kerberos.kinit.cmd", "");
    kafka_config.set("sasl.kerberos.min.time.before.relogin", "0");

    if (kafka_config.has_property("sasl.kerberos.keytab") && kafka_config.has_property("sasl.kerberos.principal"))
    {
        String keytab = kafka_config.get("sasl.kerberos.keytab");
        String principal = kafka_config.get("sasl.kerberos.principal");
        LOG_DEBUG(params.log, "Running KerberosInit");
        try
        {
            kerberosInit(keytab, principal);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(params.log, "KerberosInit failure: {}", getExceptionMessage(e, false));
        }
        LOG_DEBUG(params.log, "Finished KerberosInit");
    }
#else // USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.keytab") || kafka_config.has_property("sasl.kerberos.principal"))
        LOG_WARNING(log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
#endif // USE_KRB5
    // No need to add any prefix, messages can be distinguished
    kafka_config.set_log_callback(
        [log = params.log](cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message)
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
        void * self = static_cast<void *>(&storage);

        int status;

        status = rd_kafka_conf_interceptor_add_on_new(
            kafka_config.get_handle(), "init", StorageKafkaInterceptors<TKafkaStorage>::rdKafkaOnNew, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(params.log, "Cannot set new interceptor due to {} error", status);

        // cppkafka always copy the configuration
        status = rd_kafka_conf_interceptor_add_on_conf_dup(
            kafka_config.get_handle(), "init", StorageKafkaInterceptors<TKafkaStorage>::rdKafkaOnConfDup, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(params.log, "Cannot set dup conf interceptor due to {} error", status);
    }
}

template <typename TKafkaStorage>
cppkafka::Configuration KafkaConfigLoader::getConsumerConfiguration(TKafkaStorage & storage, const ConsumerConfigParams & params)
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", params.brokers);
    conf.set("group.id", params.group);
    if (params.multiple_consumers)
        conf.set("client.id", fmt::format("{}-{}", params.client_id, params.consumer_number));
    else
        conf.set("client.id", params.client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    conf.set("auto.offset.reset", "earliest"); // If no offset stored for this group, read all messages from the start

    // that allows to prevent fast draining of the librdkafka queue
    // during building of single insert block. Improves performance
    // significantly, but may lead to bigger memory consumption.
    size_t default_queued_min_messages = 100000; // must be greater than or equal to default
    size_t max_allowed_queued_min_messages = 10000000; // must be less than or equal to max allowed value
    conf.set(
        "queued.min.messages", std::min(std::max(params.max_block_size, default_queued_min_messages), max_allowed_queued_min_messages));

    updateGlobalConfiguration(conf, storage, params);
    loadConsumerConfig(conf, params);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false"); // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false"); // Ignore EOF messages

    for (auto & property : conf.get_all())
    {
        LOG_TRACE(params.log, "Consumer set property {}:{}", property.first, property.second);
    }

    return conf;
}

template cppkafka::Configuration KafkaConfigLoader::getConsumerConfiguration<StorageKafka>(StorageKafka & storage, const ConsumerConfigParams & params);
template cppkafka::Configuration KafkaConfigLoader::getConsumerConfiguration<StorageKafka2>(StorageKafka2 & storage, const ConsumerConfigParams & params);

template <typename TKafkaStorage>
cppkafka::Configuration KafkaConfigLoader::getProducerConfiguration(TKafkaStorage & storage, const ProducerConfigParams & params)
{
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", params.brokers);
    conf.set("client.id", params.client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);

    updateGlobalConfiguration(conf, storage, params);
    loadProducerConfig(conf, params);

    for (auto & property : conf.get_all())
    {
        LOG_TRACE(params.log, "Producer set property {}:{}", property.first, property.second);
    }

    return conf;
}

template cppkafka::Configuration KafkaConfigLoader::getProducerConfiguration<StorageKafka>(StorageKafka & storage, const ProducerConfigParams & params);
template cppkafka::Configuration KafkaConfigLoader::getProducerConfiguration<StorageKafka2>(StorageKafka2 & storage, const ProducerConfigParams & params);

void registerStorageKafka(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args) -> std::shared_ptr<IStorage>
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        const bool has_settings = args.storage_def->settings;

        auto kafka_settings = std::make_unique<KafkaSettings>();
        String collection_name;
        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext()))
        {
            for (const auto & setting : kafka_settings->all())
            {
                const auto & setting_name = setting.getName();
                if (named_collection->has(setting_name))
                    kafka_settings->set(setting_name, named_collection->get<String>(setting_name));
            }
            collection_name = assert_cast<const ASTIdentifier *>(args.engine_args[0].get())->name();
        }

        if (has_settings)
        {
            kafka_settings->loadFromQuery(*args.storage_def);
        }

// Check arguments and settings
#define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME, EVAL) \
    /* One of the four required arguments is not specified */ \
    if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 && !kafka_settings->PAR_NAME.changed) \
    { \
        throw Exception( \
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, \
            "Required parameter '{}' " \
            "for storage Kafka not specified", \
            #PAR_NAME); \
    } \
    if (args_count >= (ARG_NUM)) \
    { \
        /* The same argument is given in two places */ \
        if (has_settings && kafka_settings->PAR_NAME.changed) \
        { \
            throw Exception( \
                ErrorCodes::BAD_ARGUMENTS, \
                "The argument №{} of storage Kafka " \
                "and the parameter '{}' " \
                "in SETTINGS cannot be specified at the same time", \
                #ARG_NUM, \
                #PAR_NAME); \
        } \
        /* move engine args to settings */ \
        else \
        { \
            if constexpr ((EVAL) == 1) \
            { \
                engine_args[(ARG_NUM)-1] = evaluateConstantExpressionAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
            } \
            if constexpr ((EVAL) == 2) \
            { \
                engine_args[(ARG_NUM)-1] \
                    = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
            } \
            kafka_settings->PAR_NAME = engine_args[(ARG_NUM)-1]->as<ASTLiteral &>().value; \
        } \
    }

        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constant expression with a string result)
          * - Message format (string)
          * - Row delimiter
          * - Schema (optional, if the format supports it)
          * - Number of consumers
          * - Max block size for background consumption
          * - Skip (at least) unreadable messages number
          * - Do intermediate commits when the batch consumed and handled
          */

        /* 0 = raw, 1 = evaluateConstantExpressionAsLiteral, 2=evaluateConstantExpressionOrIdentifierAsLiteral */
        /// In case of named collection we already validated the arguments.
        if (collection_name.empty())
        {
            CHECK_KAFKA_STORAGE_ARGUMENT(1, kafka_broker_list, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(2, kafka_topic_list, 1)
            CHECK_KAFKA_STORAGE_ARGUMENT(3, kafka_group_name, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(4, kafka_format, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(5, kafka_row_delimiter, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(6, kafka_schema, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(7, kafka_num_consumers, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(8, kafka_max_block_size, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(9, kafka_skip_broken_messages, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(10, kafka_commit_every_batch, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(11, kafka_client_id, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(12, kafka_poll_timeout_ms, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(13, kafka_flush_interval_ms, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(14, kafka_thread_per_consumer, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(15, kafka_handle_error_mode, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(16, kafka_commit_on_select, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(17, kafka_max_rows_per_message, 0)
        }

#undef CHECK_KAFKA_STORAGE_ARGUMENT

        auto num_consumers = kafka_settings->kafka_num_consumers.value;
        auto max_consumers = std::max<uint32_t>(getNumberOfPhysicalCPUCores(), 16);

        if (!args.getLocalContext()->getSettingsRef().kafka_disable_num_consumers_limit && num_consumers > max_consumers)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The number of consumers can not be bigger than {}. "
                "A single consumer can read any number of partitions. "
                "Extra consumers are relatively expensive, "
                "and using a lot of them can lead to high memory and CPU usage. "
                "To achieve better performance "
                "of getting data from Kafka, consider using a setting kafka_thread_per_consumer=1, "
                "and ensure you have enough threads "
                "in MessageBrokerSchedulePool (background_message_broker_schedule_pool_size). "
                "See also https://clickhouse.com/docs/integrations/kafka/kafka-table-engine#tuning-performance",
                max_consumers);
        }
        else if (num_consumers < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of consumers can not be lower than 1");
        }

        if (kafka_settings->kafka_max_block_size.changed && kafka_settings->kafka_max_block_size.value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_max_block_size can not be lower than 1");
        }

        if (kafka_settings->kafka_poll_max_batch_size.changed && kafka_settings->kafka_poll_max_batch_size.value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_poll_max_batch_size can not be lower than 1");
        }
        NamesAndTypesList supported_columns;
        for (const auto & column : args.columns)
        {
            if (column.default_desc.kind == ColumnDefaultKind::Alias)
                supported_columns.emplace_back(column.name, column.type);
            if (column.default_desc.kind == ColumnDefaultKind::Default && !column.default_desc.expression)
                supported_columns.emplace_back(column.name, column.type);
        }
        // Kafka engine allows only ordinary columns without default expression or alias columns.
        if (args.columns.getAll() != supported_columns)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "KafkaEngine doesn't support DEFAULT/MATERIALIZED/EPHEMERAL expressions for columns. "
                "See https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka/#configuration");
        }

        const auto has_keeper_path = kafka_settings->kafka_keeper_path.changed && !kafka_settings->kafka_keeper_path.value.empty();
        const auto has_replica_name = kafka_settings->kafka_replica_name.changed && !kafka_settings->kafka_replica_name.value.empty();

        if (!has_keeper_path && !has_replica_name)
            return std::make_shared<StorageKafka>(
                args.table_id, args.getContext(), args.columns, args.comment, std::move(kafka_settings), collection_name);

        if (!args.getLocalContext()->getSettingsRef().allow_experimental_kafka_offsets_storage_in_keeper && !args.query.attach)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Storing the Kafka offsets in Keeper is experimental. Set `allow_experimental_kafka_offsets_storage_in_keeper` setting "
                "to enable it");

        if (!has_keeper_path || !has_replica_name)
            throw Exception(
        ErrorCodes::BAD_ARGUMENTS, "Either specify both zookeeper path and replica name or none of them");

        const auto is_on_cluster = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        const auto is_replicated_database = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
            && DatabaseCatalog::instance().getDatabase(args.table_id.database_name)->getEngineName() == "Replicated";

        // UUID macro is only allowed:
        // - with Atomic database only with ON CLUSTER queries, otherwise it is easy to misuse: each replica would have separate uuid generated.
        // - with Replicated database
        // - with attach queries, as those are used on server startup
        const auto allow_uuid_macro = is_on_cluster || is_replicated_database || args.query.attach;

        auto context = args.getContext();
        // Unfold {database} and {table} macro on table creation, so table can be renamed.
        if (args.mode < LoadingStrictnessLevel::ATTACH)
        {
            Macros::MacroExpansionInfo info;
            /// NOTE: it's not recursive
            info.expand_special_macros_only = true;
            info.table_id = args.table_id;
            // We could probably unfold UUID here too, but let's keep it similar to ReplicatedMergeTree, which doesn't do the unfolding.
            info.table_id.uuid = UUIDHelpers::Nil;
            kafka_settings->kafka_keeper_path.value = context->getMacros()->expand(kafka_settings->kafka_keeper_path.value, info);

            info.level = 0;
            kafka_settings->kafka_replica_name.value = context->getMacros()->expand(kafka_settings->kafka_replica_name.value, info);
        }


        auto * settings_query = args.storage_def->settings;
        chassert(has_settings && "Unexpected settings query in StorageKafka");

        settings_query->changes.setSetting("kafka_keeper_path", kafka_settings->kafka_keeper_path.value);
        settings_query->changes.setSetting("kafka_replica_name", kafka_settings->kafka_replica_name.value);

        // Expand other macros (such as {replica}). We do not expand them on previous step to make possible copying metadata files between replicas.
        // Disable expanding {shard} macro, because it can lead to incorrect behavior and it doesn't make sense to shard Kafka tables.
        Macros::MacroExpansionInfo info;
        info.table_id = args.table_id;
        if (is_replicated_database)
        {
            auto database = DatabaseCatalog::instance().getDatabase(args.table_id.database_name);
            info.shard.reset();
            info.replica = getReplicatedDatabaseReplicaName(database);
        }
        if (!allow_uuid_macro)
            info.table_id.uuid = UUIDHelpers::Nil;
        kafka_settings->kafka_keeper_path.value = context->getMacros()->expand(kafka_settings->kafka_keeper_path.value, info);

        info.level = 0;
        info.table_id.uuid = UUIDHelpers::Nil;
        kafka_settings->kafka_replica_name.value = context->getMacros()->expand(kafka_settings->kafka_replica_name.value, info);

        return std::make_shared<StorageKafka2>(
            args.table_id, args.getContext(), args.columns, args.comment, std::move(kafka_settings), collection_name);
    };

    factory.registerStorage(
        "Kafka",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

namespace StorageKafkaUtils
{
Names parseTopics(String topic_list)
{
    Names result;
    boost::split(result, topic_list, [](char c) { return c == ','; });
    for (String & topic : result)
        boost::trim(topic);
    return result;
}

String getDefaultClientId(const StorageID & table_id)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id.database_name, table_id.table_name);
}

void drainConsumer(
    cppkafka::Consumer & consumer, const std::chrono::milliseconds drain_timeout, const LoggerPtr & log, ErrorHandler error_handler)
{
    auto start_time = std::chrono::steady_clock::now();
    cppkafka::Error last_error(RD_KAFKA_RESP_ERR_NO_ERROR);

    while (true)
    {
        auto msg = consumer.poll(100ms);
        if (!msg)
            break;

        auto error = msg.get_error();

        if (error)
        {
            if (msg.is_eof() || error == last_error)
            {
                break;
            }
            else
            {
                LOG_ERROR(log, "Error during draining: {}", error);
                error_handler(error);
            }
        }

        // i don't stop draining on first error,
        // only if it repeats once again sequentially
        last_error = error;

        auto ts = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time) > drain_timeout)
        {
            LOG_ERROR(log, "Timeout during draining.");
            break;
        }
    }
}

void eraseMessageErrors(Messages & messages, const LoggerPtr & log, ErrorHandler error_handler)
{
    size_t skipped = std::erase_if(
        messages,
        [&](auto & message)
        {
            if (auto error = message.get_error())
            {
                ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
                LOG_ERROR(log, "Consumer error: {}", error);
                error_handler(error);
                return true;
            }
            return false;
        });

    if (skipped)
        LOG_ERROR(log, "There were {} messages with an error", skipped);
}

SettingsChanges createSettingsAdjustments(KafkaSettings & kafka_settings, const String & schema_name)
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (!kafka_settings.input_format_skip_unknown_fields.changed)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        kafka_settings.input_format_skip_unknown_fields = true;
    }

    if (!kafka_settings.input_format_allow_errors_ratio.changed)
    {
        kafka_settings.input_format_allow_errors_ratio = 0.;
    }

    if (!kafka_settings.input_format_allow_errors_num.changed)
    {
        kafka_settings.input_format_allow_errors_num = kafka_settings.kafka_skip_broken_messages.value;
    }

    if (!schema_name.empty())
        result.emplace_back("format_schema", schema_name);

    for (const auto & setting : kafka_settings)
    {
        const auto & name = setting.getName();
        if (name.find("kafka_") == std::string::npos)
            result.emplace_back(name, setting.getValue());
    }
    return result;
}


bool checkDependencies(const StorageID & table_id, const ContextPtr& context)
{
    // Check if all dependencies are attached
    auto view_ids = DatabaseCatalog::instance().getDependentViews(table_id);
    if (view_ids.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & view_id : view_ids)
    {
        auto view = DatabaseCatalog::instance().tryGetTable(view_id, context);
        if (!view)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(view.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(view_id, context))
            return false;
    }

    return true;
}


VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode)
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
}

template struct StorageKafkaInterceptors<StorageKafka>;
template struct StorageKafkaInterceptors<StorageKafka2>;

}
