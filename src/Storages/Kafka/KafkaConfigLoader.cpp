#include <Storages/Kafka/KafkaConfigLoader.h>

#include <Access/KerberosInit.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ThreadStatus.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>

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

namespace KafkaSetting
{
    extern const KafkaSettingsString kafka_security_protocol;
    extern const KafkaSettingsString kafka_sasl_mechanism;
    extern const KafkaSettingsString kafka_sasl_username;
    extern const KafkaSettingsString kafka_sasl_password;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename TKafkaStorage>
struct KafkaInterceptors
{
    static rd_kafka_resp_err_t rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx);

    static rd_kafka_resp_err_t rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx);

    static rd_kafka_resp_err_t
    rdKafkaOnNew(rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/);

    static rd_kafka_resp_err_t rdKafkaOnConfDup(
        rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx);
};

template <typename TStorageKafka>
rd_kafka_resp_err_t
KafkaInterceptors<TStorageKafka>::rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx)
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
rd_kafka_resp_err_t KafkaInterceptors<TStorageKafka>::rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    CurrentMetrics::sub(CurrentMetrics::KafkaLibrdkafkaThreads, 1);

    std::lock_guard lock(self->thread_statuses_mutex);
    const auto it = std::find_if(
        self->thread_statuses.begin(),
        self->thread_statuses.end(),
        [](const auto & thread_status_ptr) { return thread_status_ptr.get() == current_thread; });
    if (it == self->thread_statuses.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No thread status for this librdkafka thread.");

    self->thread_statuses.erase(it);

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t KafkaInterceptors<TStorageKafka>::rdKafkaOnNew(
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
rd_kafka_resp_err_t KafkaInterceptors<TStorageKafka>::rdKafkaOnConfDup(
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

template struct KafkaInterceptors<StorageKafka>;
template struct KafkaInterceptors<StorageKafka2>;

namespace
{

void setKafkaConfigValue(cppkafka::Configuration & kafka_config, const String & key, const String & value)
{
    /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
    /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    const String setting_name_in_kafka_config = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
    kafka_config.set(setting_name_in_kafka_config, value);
}

void loadConfigProperty(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & tag)
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

void loadLegacyTopicConfig(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const String & config_prefix)
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
void loadTopicConfig(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const String & config_prefix,
    const String & topic)
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
void loadFromConfig(
    cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix)
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

        if (tag.starts_with(
                KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG)) /// multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
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

void loadConsumerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String consumer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, consumer_path);
}

void loadProducerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String producer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_PRODUCER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, producer_path);
}

template <typename TKafkaStorage>
void updateGlobalConfiguration(
    cppkafka::Configuration & kafka_config,
    TKafkaStorage & storage,
    const KafkaConfigLoader::LoadConfigParams & params,
    IKafkaExceptionInfoSinkWeakPtr exception_info_sink_ptr = IKafkaExceptionInfoSinkWeakPtr())
{
    loadFromConfig(kafka_config, params, KafkaConfigLoader::CONFIG_KAFKA_TAG);

    auto kafka_settings = storage.getKafkaSettings();
    if (!kafka_settings[KafkaSetting::kafka_security_protocol].value.empty())
        kafka_config.set("security.protocol", kafka_settings[KafkaSetting::kafka_security_protocol]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_mechanism].value.empty())
        kafka_config.set("sasl.mechanism", kafka_settings[KafkaSetting::kafka_sasl_mechanism]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_username].value.empty())
        kafka_config.set("sasl.username", kafka_settings[KafkaSetting::kafka_sasl_username]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_password].value.empty())
        kafka_config.set("sasl.password", kafka_settings[KafkaSetting::kafka_sasl_password]);

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
        catch (Exception & e)
        {
            LOG_ERROR(params.log, "KerberosInit failure: {}", getExceptionMessageForLogging(e, false));
        }
        LOG_DEBUG(params.log, "Finished KerberosInit");
    }
#else // USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.keytab") || kafka_config.has_property("sasl.kerberos.principal"))
        LOG_WARNING(params.log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
#endif // USE_KRB5
    // No need to add any prefix, messages can be distinguished
    kafka_config.set_log_callback(
        [log = params.log, sink = exception_info_sink_ptr](
            cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message)
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
            if (client_logs_level <= DB::LogsLevel::error)
            {
                if (auto sink_shared_ptr = sink.lock())
                {
                    ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
                    sink_shared_ptr->setExceptionInfo(message, /* with_stacktrace = */ true);
                }
            }
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

        status
            = rd_kafka_conf_interceptor_add_on_new(kafka_config.get_handle(), "init", KafkaInterceptors<TKafkaStorage>::rdKafkaOnNew, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(params.log, "Cannot set new interceptor due to {} error", status);

        // cppkafka always copy the configuration
        status = rd_kafka_conf_interceptor_add_on_conf_dup(
            kafka_config.get_handle(), "init", KafkaInterceptors<TKafkaStorage>::rdKafkaOnConfDup, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(params.log, "Cannot set dup conf interceptor due to {} error", status);
    }
}

}

template <typename TKafkaStorage>
cppkafka::Configuration KafkaConfigLoader::getConsumerConfiguration(TKafkaStorage & storage, const ConsumerConfigParams & params, IKafkaExceptionInfoSinkPtr exception_info_sink_ptr)
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

    updateGlobalConfiguration(conf, storage, params, exception_info_sink_ptr);
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

template cppkafka::Configuration
KafkaConfigLoader::getConsumerConfiguration<StorageKafka>(StorageKafka & storage, const ConsumerConfigParams & params, IKafkaExceptionInfoSinkPtr exception_info_sink_ptr);
template cppkafka::Configuration
KafkaConfigLoader::getConsumerConfiguration<StorageKafka2>(StorageKafka2 & storage, const ConsumerConfigParams & params, IKafkaExceptionInfoSinkPtr exception_info_sink_ptr);

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

template cppkafka::Configuration
KafkaConfigLoader::getProducerConfiguration<StorageKafka>(StorageKafka & storage, const ProducerConfigParams & params);
template cppkafka::Configuration
KafkaConfigLoader::getProducerConfiguration<StorageKafka2>(StorageKafka2 & storage, const ProducerConfigParams & params);

}
