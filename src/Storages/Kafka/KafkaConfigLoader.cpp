#include <Storages/Kafka/KafkaConfigLoader.h>

#include <Access/KerberosInit.h>
#include <Storages/Kafka/AWSMSKIAMAuth.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <Storages/System/StorageSystemStackTrace.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/HiddenSecret.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/QueryProfiler.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/getAvailabilityZone.h>
#include <chrdkafka_conf_sensitive.h>
#include <csignal>
#include <unordered_set>

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
    extern const KafkaSettingsString kafka_compression_codec;
    extern const KafkaSettingsInt64 kafka_compression_level;
    extern const KafkaSettingsString kafka_autodetect_client_rack;
    extern const KafkaSettingsString kafka_aws_region;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
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
            DB::setThreadName(ThreadName::KAFKA_MAIN);
            break;
        case RD_KAFKA_THREAD_BACKGROUND:
            DB::setThreadName(ThreadName::KAFKA_BACKGROUND);
            break;
        case RD_KAFKA_THREAD_BROKER:
            DB::setThreadName(ThreadName::KAFKA_BROKER);
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

    /// Due to [1] librdkafka blocks all signals before creating threads,
    /// and broker threads are created while signals are already all-blocked
    /// (inside rd_kafka_new), so they inherit the all-blocked mask.
    /// We unblock only the specific signals needed by `system.stack_trace`
    /// (STACK_TRACE_SERVICE_SIGNAL) and the query profiler (SIGUSR1/SIGUSR2),
    /// rather than the full mask — otherwise we would also drop the process-wide
    /// SIGPIPE block installed by the daemon.
    ///
    ///   [1]: https://github.com/confluentinc/librdkafka/issues/4571
    sigset_t mask;
    sigemptyset(&mask);
#if defined(OS_LINUX) || defined(OS_DARWIN)
    sigaddset(&mask, STACK_TRACE_SERVICE_SIGNAL);
#endif
    sigaddset(&mask, QueryProfilerReal::PAUSE_SIGNAL);
    sigaddset(&mask, QueryProfilerCPU::PAUSE_SIGNAL);
    pthread_sigmask(SIG_UNBLOCK, &mask, nullptr);

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
    rd_kafka_resp_err_t status = {};

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
    rd_kafka_resp_err_t status = {};

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

namespace
{

template <typename TConfig>
void setKafkaConfigValue(TConfig & kafka_config, const String & key, const String & value)
{
    /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
    /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    /// ClickHouse-specific keys that live under <kafka> in server config but are not librdkafka properties.
    /// Exclude them here so they are not forwarded to cppkafka.
    static const std::unordered_set<String> clickhouse_only_kafka_config_keys
        = {"use_environment_credentials"}; /// AWS MSK IAM: controls AWS credentials provider selection
    if (clickhouse_only_kafka_config_keys.contains(key))
        return;

    const String setting_name_in_kafka_config = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
    kafka_config.set(setting_name_in_kafka_config, value);
}

template <typename TConfig>
void loadConfigProperty(
    TConfig & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & tag)
{
    const String property_path = config_prefix + "." + tag;
    const String property_value = config.getString(property_path);

    setKafkaConfigValue(kafka_config, tag, property_value);
}

template <typename TConfig>
void loadNamedCollectionConfig(TConfig & kafka_config, const String & collection_name, const String & config_prefix)
{
    const auto & collection = NamedCollectionFactory::instance().get(collection_name);
    for (const auto & key : collection->getKeys(-1, config_prefix))
    {
        // Cut prefix with '.' before actual config tag.
        const auto param_name = key.substr(config_prefix.size() + 1);
        setKafkaConfigValue(kafka_config, param_name, collection->get<String>(key));
    }
}

template <typename TConfig>
void loadLegacyTopicConfig(
    TConfig & kafka_config,
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
template <typename TConfig>
void loadTopicConfig(
    TConfig & kafka_config,
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
template <typename TConfig>
void loadFromConfig(TConfig & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix)
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

template <typename TConfig>
void loadLegacyConfigSyntax(
    TConfig & kafka_config,
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

template <typename TConfig>
void loadConsumerConfig(TConfig & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String consumer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, consumer_path);
}

template <typename TConfig>
void loadProducerConfig(TConfig & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String producer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_PRODUCER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, producer_path);
}

template <typename TKafkaStorage>
using SpecificConfigUpdaterFunc = void (*)(
    cppkafka::Configuration & kafka_config,
    const KafkaConfigLoader::LoadConfigParams & params);

template <typename TKafkaStorage>
void updateConfigurationFromConfig(
    SpecificConfigUpdaterFunc<TKafkaStorage> specific_config_updater,
    cppkafka::Configuration & kafka_config,
    TKafkaStorage & storage,
    const KafkaConfigLoader::LoadConfigParams & params,
    IKafkaExceptionInfoSinkWeakPtr exception_info_sink_ptr = IKafkaExceptionInfoSinkWeakPtr())
{
    loadFromConfig(kafka_config, params, KafkaConfigLoader::CONFIG_KAFKA_TAG);

    specific_config_updater(kafka_config, params);

    auto kafka_settings = storage.getKafkaSettings();
    if (!kafka_settings[KafkaSetting::kafka_security_protocol].value.empty())
        kafka_config.set("security.protocol", kafka_settings[KafkaSetting::kafka_security_protocol]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_mechanism].value.empty()
        && !boost::iequals(kafka_settings[KafkaSetting::kafka_sasl_mechanism].value, "AWS_MSK_IAM"))
        kafka_config.set("sasl.mechanism", kafka_settings[KafkaSetting::kafka_sasl_mechanism]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_username].value.empty())
        kafka_config.set("sasl.username", kafka_settings[KafkaSetting::kafka_sasl_username]);
    if (!kafka_settings[KafkaSetting::kafka_sasl_password].value.empty())
        kafka_config.set("sasl.password", kafka_settings[KafkaSetting::kafka_sasl_password]);
    if (!kafka_settings[KafkaSetting::kafka_compression_codec].value.empty())
        kafka_config.set("compression.codec", kafka_settings[KafkaSetting::kafka_compression_codec]);

    if (kafka_settings[KafkaSetting::kafka_compression_level].changed)
        kafka_config.set("compression.level", kafka_settings[KafkaSetting::kafka_compression_level].toString());

    auto autodetect_rack = kafka_settings[KafkaSetting::kafka_autodetect_client_rack].value;
    if (!autodetect_rack.empty())
    {
        if (magic_enum::enum_contains<S3::AZFacilities>(autodetect_rack))
        {
            std::string rack
                = S3::tryGetRunningAvailabilityZone(magic_enum::enum_cast<S3::AZFacilities>(autodetect_rack).value());
            if (!rack.empty())
            {
                kafka_config.set("client.rack", rack);
                LOG_TRACE(params.log, "client.rack set to {}.", rack);
            }
            else
                LOG_ERROR(params.log, "Failed to determine client.rack via facility {}.", autodetect_rack);
        }
        else
            LOG_ERROR(params.log, "Unknown kafka_autodetect_client_rack facility  {}. Expected one of AWS_ZONE_ID, AWS_ZONE_NAME, GCP_ZONE, CLICKHOUSE, AWS_ZONE_NAME_THEN_GCP_ZONE.", autodetect_rack);
    }

    /// Derive effective SASL mechanism from both table settings and server/named-collection config.
    /// Table settings take priority; fall back to whatever loadFromConfig already wrote to kafka_config.
    String sasl_mechanism = kafka_settings[KafkaSetting::kafka_sasl_mechanism].value;
    if (sasl_mechanism.empty() && kafka_config.has_property("sasl.mechanism"))
        sasl_mechanism = kafka_config.get("sasl.mechanism");

    if (boost::iequals(sasl_mechanism, "AWS_MSK_IAM"))
    {
#if USE_AWS_S3
        if (kafka_config.has_property("security.protocol")
            && !boost::iequals(kafka_config.get("security.protocol"), "SASL_SSL"))
            LOG_WARNING(
                params.log,
                "kafka_security_protocol='{}' will be overridden to 'SASL_SSL' - AWS MSK IAM requires SASL_SSL.",
                kafka_config.get("security.protocol"));

        String aws_region = kafka_settings[KafkaSetting::kafka_aws_region].value;
        String broker_list = kafka_config.has_property("metadata.broker.list") ? kafka_config.get("metadata.broker.list") : "";

        std::shared_ptr<AWSMSKIAMAuth::OAuthBearerTokenRefreshContext> candidate;
        AWSMSKIAMAuth::setupAuthentication(kafka_config, params.config, aws_region, broker_list, params.log, candidate);
        auto shared_context = storage.ensureOAuthContext(candidate);
        if (shared_context != candidate)
            AWSMSKIAMAuth::setupAuthentication(kafka_config, params.config, aws_region, broker_list, params.log, shared_context);
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "AWS MSK IAM authentication is not supported in this build. ClickHouse must be built with USE_AWS_S3=1");
#endif
    }

#if USE_KRB5
    static const String default_kinit_cmd = cppkafka::Configuration{}.get("sasl.kerberos.kinit.cmd");
    if (kafka_config.get("sasl.kerberos.kinit.cmd") != default_kinit_cmd)
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
                    // librdkafka-originated errors (auth failures, broker disconnects) have no
                    // useful ClickHouse stack trace - the trace only shows poll->log_callback.
                    // Skip stack trace to reduce noise in system.kafka_consumers.exceptions.
                    sink_shared_ptr->setExceptionInfo(message, /* with_stacktrace = */ false);
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

        int status = 0;

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

namespace
{

/// Sensitive properties must not be logged in cleartext: the log records can reach not only the
/// server log, but also clients that set `send_logs_level`.
bool isSensitiveProperty(std::string_view name)
{
    /// The properties librdkafka marks with the _RK_SENSITIVE flag, plus a substring safety net
    /// for properties unknown to the vendored librdkafka version.
    static const std::unordered_set<std::string_view> sensitive_properties = []
    {
        std::unordered_set<std::string_view> res;
        for (const char * const * prop_name = chrd_kafka_conf_sensitive_properties(); *prop_name; ++prop_name)
            res.emplace(*prop_name);
        return res;
    }();
    return sensitive_properties.contains(name) || name.contains("password") || name.contains("secret");
}

/// Log all properties of a Kafka client configuration, replacing the values of sensitive
/// properties, e.g. `sasl.password` or `sasl.oauthbearer.client.secret`, with `[HIDDEN]`.
void logConfigProperties(const cppkafka::Configuration & conf, const LoggerPtr & log, std::string_view client_type)
{
    for (const auto & property : conf.get_all())
        LOG_TRACE(log, "{} set property {}:{}", client_type, property.first,
            isSensitiveProperty(property.first) ? HIDDEN_SECRET : std::string_view(property.second));
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

    updateConfigurationFromConfig(loadConsumerConfig, conf, storage, params, exception_info_sink_ptr);

    /// Re-validate the broker list in case they are changed in the merged configuration
    if (const auto merged_broker_list = conf.get("metadata.broker.list"); merged_broker_list != params.brokers)
        conf.set("metadata.broker.list", StorageKafkaUtils::validateBrokerList(merged_broker_list, params.context));

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false"); // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false"); // Ignore EOF messages

    logConfigProperties(conf, params.log, "Consumer");

    return conf;
}

template cppkafka::Configuration
KafkaConfigLoader::getConsumerConfiguration<StorageKafka>(StorageKafka & storage, const ConsumerConfigParams & params, IKafkaExceptionInfoSinkPtr exception_info_sink_ptr);

template <typename TKafkaStorage>
cppkafka::Configuration KafkaConfigLoader::getProducerConfiguration(TKafkaStorage & storage, const ProducerConfigParams & params)
{
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", params.brokers);
    conf.set("client.id", params.client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);

    updateConfigurationFromConfig(loadProducerConfig, conf, storage, params);

    /// See the same check in getConsumerConfiguration.
    if (const auto merged_broker_list = conf.get("metadata.broker.list"); merged_broker_list != params.brokers)
        conf.set("metadata.broker.list", StorageKafkaUtils::validateBrokerList(merged_broker_list, params.context));

    logConfigProperties(conf, params.log, "Producer");

    /// compression.codec is a global and topic level property, however compression.level is only a topic level property.
    /// cppkafka::Configuration::get_all returns the global properties only, so we need to check compression.level separately.
    /// It is not clear why compression.level is like this, but let's work around it.
    const std::string compression_level_key = "compression.level";
    if (conf.has_property(compression_level_key))
        LOG_TRACE(params.log, "Producer set property {}:{}", compression_level_key, conf.get(compression_level_key));

    return conf;
}

template cppkafka::Configuration
KafkaConfigLoader::getProducerConfiguration<StorageKafka>(StorageKafka & storage, const ProducerConfigParams & params);

namespace
{

/// Collects the raw librdkafka-style properties from the server configuration
/// (via the same traversal the legacy loader uses) so the Kafka2 engine can map
/// the supported subset onto the in-tree client and warn about the rest.
struct RawKafkaProperties
{
    /// Ordered, so precedence and warnings are deterministic.
    std::map<String, String> properties;

    void set(const String & name, const String & value) { properties[name] = value; }
};

Kafka::Compression parseCompressionCodec(const String & value)
{
    if (boost::iequals(value, "none"))
        return Kafka::Compression::None;
    if (boost::iequals(value, "gzip"))
        return Kafka::Compression::Gzip;
    if (boost::iequals(value, "snappy"))
        return Kafka::Compression::Snappy;
    if (boost::iequals(value, "lz4"))
        return Kafka::Compression::Lz4;
    if (boost::iequals(value, "zstd"))
        return Kafka::Compression::Zstd;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Kafka compression codec '{}'", value);
}

Kafka::SecurityProtocol parseSecurityProtocol(const String & value)
{
    if (boost::iequals(value, "plaintext"))
        return Kafka::SecurityProtocol::Plaintext;
    if (boost::iequals(value, "ssl"))
        return Kafka::SecurityProtocol::Ssl;
    if (boost::iequals(value, "sasl_plaintext"))
        return Kafka::SecurityProtocol::SaslPlaintext;
    if (boost::iequals(value, "sasl_ssl"))
        return Kafka::SecurityProtocol::SaslSsl;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Kafka security protocol '{}'", value);
}

}

KafkaConfigLoader::Kafka2Configuration KafkaConfigLoader::getKafka2Configuration(StorageKafka2 & storage, const Kafka2ConfigParams & params)
{
    /// Gather the raw properties exactly like the legacy loader: the global
    /// <kafka> section, the legacy and new per-topic sections, and the
    /// consumer/producer sections. The in-tree client is one unified handle,
    /// so the consumer and producer sections are merged.
    RawKafkaProperties raw;
    loadFromConfig(raw, params, CONFIG_KAFKA_TAG);
    loadConsumerConfig(raw, params);
    loadProducerConfig(raw, params);

    String brokers = params.brokers;
    String client_id = params.multiple_consumers ? fmt::format("{}-{}", params.client_id, params.consumer_number) : params.client_id;
    String protocol_name;
    String mechanism_name;
    String sasl_username;
    String sasl_password;
    String kerberos_service_name = "kafka";
    String kerberos_keytab;
    String kerberos_principal;
    Kafka::TlsConfig tls;
    bool has_tls_settings = false;
    std::optional<String> compression_codec;
    std::optional<Int64> compression_level;

    for (const auto & [name, value] : raw.properties)
    {
        if (name == "security.protocol")
            protocol_name = value;
        else if (name == "sasl.mechanism" || name == "sasl.mechanisms")
            mechanism_name = value;
        else if (name == "sasl.username")
            sasl_username = value;
        else if (name == "sasl.password")
            sasl_password = value;
        else if (name == "sasl.kerberos.service.name")
            kerberos_service_name = value;
        else if (name == "sasl.kerberos.keytab")
            kerberos_keytab = value;
        else if (name == "sasl.kerberos.principal")
            kerberos_principal = value;
        else if (name == "ssl.ca.location")
        {
            tls.ca_location = value;
            has_tls_settings = true;
        }
        else if (name == "ssl.certificate.location")
        {
            tls.certificate_location = value;
            has_tls_settings = true;
        }
        else if (name == "ssl.key.location")
        {
            tls.key_location = value;
            has_tls_settings = true;
        }
        else if (name == "ssl.key.password")
        {
            tls.key_password = value;
            has_tls_settings = true;
        }
        else if (name == "enable.ssl.certificate.verification")
        {
            tls.verify = boost::iequals(value, "true") || value == "1";
            has_tls_settings = true;
        }
        else if (name == "client.id")
            client_id = value;
        else if (name == "metadata.broker.list" || name == "bootstrap.servers")
            brokers = StorageKafkaUtils::validateBrokerList(value, params.context);
        else if (name == "compression.codec" || name == "compression.type")
            compression_codec = value;
        else if (name == "compression.level")
            compression_level = parse<Int64>(value);
        else
            LOG_WARNING(
                params.log, "Kafka configuration property '{}' is not supported by the Kafka2 engine and is ignored", name);
    }

    /// Table SETTINGS take priority over the server configuration (same
    /// precedence as the legacy loader).
    const auto & kafka_settings = storage.getKafkaSettings();
    if (!kafka_settings[KafkaSetting::kafka_security_protocol].value.empty())
        protocol_name = kafka_settings[KafkaSetting::kafka_security_protocol].value;
    if (!kafka_settings[KafkaSetting::kafka_sasl_mechanism].value.empty())
        mechanism_name = kafka_settings[KafkaSetting::kafka_sasl_mechanism].value;
    if (!kafka_settings[KafkaSetting::kafka_sasl_username].value.empty())
        sasl_username = kafka_settings[KafkaSetting::kafka_sasl_username].value;
    if (!kafka_settings[KafkaSetting::kafka_sasl_password].value.empty())
        sasl_password = kafka_settings[KafkaSetting::kafka_sasl_password].value;
    if (!kafka_settings[KafkaSetting::kafka_compression_codec].value.empty())
        compression_codec = kafka_settings[KafkaSetting::kafka_compression_codec].value;
    if (kafka_settings[KafkaSetting::kafka_compression_level].changed)
        compression_level = kafka_settings[KafkaSetting::kafka_compression_level].value;
    if (!kafka_settings[KafkaSetting::kafka_autodetect_client_rack].value.empty())
        LOG_WARNING(params.log, "Setting kafka_autodetect_client_rack is not supported by the Kafka2 engine and is ignored");

    Kafka2Configuration result;
    auto & client = result.client;

    for (size_t start = 0; start < brokers.size();)
    {
        const size_t comma = brokers.find(',', start);
        String entry = brokers.substr(start, comma == String::npos ? String::npos : comma - start);
        boost::trim(entry);
        if (!entry.empty())
            client.bootstrap_brokers.push_back(std::move(entry));
        if (comma == String::npos)
            break;
        start = comma + 1;
    }
    client.client_id = client_id;
    client.log = params.log;
    /// The librdkafka-based engine refreshed the topic metadata every minute
    /// (`topic.metadata.refresh.interval.ms = 60000`).
    client.metadata_max_age = std::chrono::minutes(1);

    const bool is_msk_iam = boost::iequals(mechanism_name, "AWS_MSK_IAM");
    auto protocol = Kafka::SecurityProtocol::Plaintext;
    if (is_msk_iam)
    {
        if (!protocol_name.empty() && !boost::iequals(protocol_name, "sasl_ssl"))
            LOG_WARNING(
                params.log, "kafka_security_protocol='{}' will be overridden to 'sasl_ssl' - AWS MSK IAM requires SASL_SSL.", protocol_name);
        protocol = Kafka::SecurityProtocol::SaslSsl;
    }
    else if (!protocol_name.empty())
    {
        protocol = parseSecurityProtocol(protocol_name);
    }
    client.security_protocol = protocol;

    /// Pass TLS settings through even for protocols that cannot use them: the
    /// client fails close on the inconsistency instead of silently ignoring
    /// security settings.
    if (has_tls_settings)
        client.tls = tls;

    if (is_msk_iam)
    {
#if USE_AWS_S3
        const String aws_region = kafka_settings[KafkaSetting::kafka_aws_region].value;
        std::shared_ptr<AWSMSKIAMAuth::OAuthBearerTokenRefreshContext> candidate;
        auto token_provider = AWSMSKIAMAuth::createTokenProvider(params.config, aws_region, brokers, params.log, candidate);
        /// Consumers and the producer of one table share the refresh context.
        auto shared_context = storage.ensureOAuthContext(candidate);
        if (shared_context != candidate)
            token_provider = AWSMSKIAMAuth::createTokenProvider(params.config, aws_region, brokers, params.log, shared_context);
        client.sasl = Kafka::SaslOauthBearerConfig{.token_provider = std::move(token_provider)};
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "AWS MSK IAM authentication is not supported in this build. ClickHouse must be built with USE_AWS_S3=1");
#endif
    }
    else if (Kafka::usesSasl(protocol))
    {
        if (mechanism_name.empty())
        {
            /// librdkafka's default `sasl.mechanism` is GSSAPI; preserved for compatibility.
            LOG_WARNING(params.log, "No SASL mechanism is configured; defaulting to GSSAPI, like librdkafka");
            mechanism_name = "GSSAPI";
        }

        if (boost::iequals(mechanism_name, "PLAIN"))
            client.sasl = Kafka::SaslPlainConfig{.username = sasl_username, .password = sasl_password, .authorization_id = {}};
        else if (boost::iequals(mechanism_name, "SCRAM-SHA-256"))
            client.sasl = Kafka::SaslScramConfig{
                .algorithm = Kafka::SaslScramConfig::Algorithm::Sha256, .username = sasl_username, .password = sasl_password};
        else if (boost::iequals(mechanism_name, "SCRAM-SHA-512"))
            client.sasl = Kafka::SaslScramConfig{
                .algorithm = Kafka::SaslScramConfig::Algorithm::Sha512, .username = sasl_username, .password = sasl_password};
        else if (boost::iequals(mechanism_name, "GSSAPI"))
        {
            client.sasl = Kafka::SaslGssapiConfig{.service_name = kerberos_service_name, .principal = kerberos_principal};
#if USE_KRB5
            if (!kerberos_keytab.empty() && !kerberos_principal.empty())
            {
                LOG_DEBUG(params.log, "Running KerberosInit");
                try
                {
                    kerberosInit(kerberos_keytab, kerberos_principal);
                }
                catch (Exception & e)
                {
                    LOG_ERROR(params.log, "KerberosInit failure: {}", getExceptionMessageForLogging(e, false));
                }
                LOG_DEBUG(params.log, "Finished KerberosInit");
            }
#else
            if (!kerberos_keytab.empty() || !kerberos_principal.empty())
                LOG_WARNING(params.log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
#endif
        }
        else if (boost::iequals(mechanism_name, "OAUTHBEARER"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Plain OAUTHBEARER is not supported by the Kafka2 engine; the only supported OAuth token source is AWS_MSK_IAM");
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SASL mechanism '{}'", mechanism_name);
    }
    else if (!mechanism_name.empty() || !sasl_username.empty() || !sasl_password.empty())
    {
        /// Fail close: silently ignored credentials hide a misconfiguration.
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A SASL mechanism or SASL credentials are configured, but the security protocol '{}' does not use SASL",
            Kafka::securityProtocolName(protocol));
    }

    if (compression_codec)
        result.compression = parseCompressionCodec(*compression_codec);
    if (compression_level)
        result.compression_level = *compression_level;

    LOG_TRACE(
        params.log,
        "Kafka2 client configuration: brokers='{}', client_id='{}', security_protocol='{}', compression='{}'",
        brokers,
        client.client_id,
        Kafka::securityProtocolName(protocol),
        compression_codec.value_or("none"));

    return result;
}

}
