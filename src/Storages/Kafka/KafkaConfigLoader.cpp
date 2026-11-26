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
#include <Common/HiddenSecret.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/QueryProfiler.h>
#include <Common/config_version.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <IO/S3/getAvailabilityZone.h>
#include <chrdkafka_conf_sensitive.h>
#include <algorithm>
#include <charconv>
#include <csignal>
#include <limits>
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
    extern const KafkaSettingsSecretString kafka_sasl_password;
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

namespace
{

/// librdkafka prints pointers with `%p`, e.g. `log_cb = 0x55d1c0a3e2f0` with `debug=conf`.
/// Its messages reach clients through `send_logs_level`, so every hexadecimal number is hidden.
std::string hideHexNumbers(std::string_view message)
{
    std::string res;
    res.reserve(message.size());
    for (size_t i = 0; i < message.size();)
    {
        if (message[i] == '0' && i + 2 < message.size() && message[i + 1] == 'x' && isHexDigit(message[i + 2]))
        {
            res += "0x";
            res += HIDDEN_SECRET;
            i += 2;
            while (i < message.size() && isHexDigit(message[i]))
                ++i;
        }
        else
            res += message[i++];
    }
    return res;
}

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
template struct KafkaInterceptors<StorageKafka2>;

namespace
{

/// librdkafka reads the name as a C string, so it must not contain a null character.
void checkKafkaPropertyName(const String & key)
{
    if (key.contains('\0'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kafka property name {} contains a null character", backQuote(key));
}

String toKafkaPropertyName(const String & key)
{
    checkKafkaPropertyName(key);
    const String name = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
    /// `test.mock.num.brokers` starts a mock cluster that uses memory addresses as member IDs.
    if (name.starts_with("test."))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kafka property {} is not allowed", name);
    return name;
}

/// Rejects the values of the allowed properties that librdkafka accepts but cannot use.
void checkSQLKafkaPropertyValue(const String & name, const String & value)
{
    if (name != "topic.metadata.propagation.max.ms")
        return;

    /// librdkafka accepts up to 3600000 but converts the value to microseconds in a 32-bit `int`: larger values overflow.
    /// The format is stricter than the one of librdkafka (which reads a prefix of the value with `strtol`),
    /// so the number that librdkafka reads is never greater than the number checked here.
    static constexpr UInt64 max_value = std::numeric_limits<int>::max() / 1000;
    UInt64 parsed = 0;
    const char * end = value.data() + value.size();
    const auto [pos, error] = std::from_chars(value.data(), end, parsed);
    if (error != std::errc() || pos != end || parsed > max_value)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Kafka property `{}` must be an integer between 0 and {}, got `{}`", name, max_value, value);
}

/// Only properties reviewed for use without access to server-local resources may be set from SQL.
/// Match the exact name passed to librdkafka, including any `topic.` prefix.
void checkSQLKafkaProperty(const String & name, const String & value)
{
    static constexpr std::string_view allowed_properties[] = {
        "acks",
        "allow.auto.create.topics",
        "api.version.request",
        "auto.commit.interval.ms",
        "auto.offset.reset",
        "batch.num.messages",
        "batch.size",
        "bootstrap.servers",
        "broker.address.family",
        "broker.address.ttl",
        "check.crcs",
        "client.id",
        "client.rack",
        "compression.codec",
        "compression.level",
        "compression.type",
        "connections.max.idle.ms",
        "coordinator.query.interval.ms",
        "debug",
        "delivery.timeout.ms",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "enable.idempotence",
        "enable.partition.eof",
        "fetch.error.backoff.ms",
        "fetch.max.bytes",
        "fetch.message.max.bytes",
        "fetch.min.bytes",
        "fetch.queue.backoff.ms",
        "fetch.wait.max.ms",
        "group.id",
        "group.instance.id",
        "group.protocol",
        "group.remote.assignor",
        "heartbeat.interval.ms",
        "https.ca.pem",
        "isolation.level",
        "linger.ms",
        "log.connection.close",
        "log_level",
        "max.in.flight",
        "max.in.flight.requests.per.connection",
        "max.partition.fetch.bytes",
        "max.poll.interval.ms",
        "message.max.bytes",
        "message.send.max.retries",
        "message.timeout.ms",
        "metadata.broker.list",
        "metadata.max.age.ms",
        "partition.assignment.strategy",
        "partitioner",
        "queue.buffering.backpressure.threshold",
        "queue.buffering.max.kbytes",
        "queue.buffering.max.messages",
        "queue.buffering.max.ms",
        "queued.max.messages.kbytes",
        "queued.min.messages",
        "receive.message.max.bytes",
        "reconnect.backoff.max.ms",
        "reconnect.backoff.ms",
        "request.required.acks",
        "request.timeout.ms",
        "retries",
        "retry.backoff.max.ms",
        "retry.backoff.ms",
        "sasl.mechanism",
        "sasl.mechanisms",
        "sasl.oauthbearer.assertion.private.key.pem",
        "sasl.password",
        "sasl.username",
        "security.protocol",
        "session.timeout.ms",
        "socket.connection.setup.timeout.ms",
        "socket.keepalive.enable",
        "socket.receive.buffer.bytes",
        "socket.send.buffer.bytes",
        "socket.timeout.ms",
        "ssl.ca.pem",
        "ssl.certificate.pem",
        "ssl.key.password",
        "ssl.key.pem",
        "statistics.interval.ms",
        "topic.acks",
        "topic.auto.offset.reset",
        "topic.compression.codec",
        "topic.compression.level",
        "topic.compression.type",
        "topic.delivery.timeout.ms",
        "topic.message.timeout.ms",
        "topic.metadata.propagation.max.ms",
        "topic.metadata.refresh.interval.ms",
        "topic.partitioner",
        "topic.request.required.acks",
        "topic.request.timeout.ms",
    };
    static_assert(std::ranges::is_sorted(allowed_properties));
    if (std::ranges::binary_search(allowed_properties, std::string_view(name)))
    {
        checkSQLKafkaPropertyValue(name, value);
        return;
    }

    /// `probe` is not a path: librdkafka searches a fixed list of the standard locations of CA certificates.
    if (value == "probe" && (name == "ssl.ca.location" || name == "https.ca.location"))
        return;

    static constexpr std::pair<std::string_view, std::string_view> pem_alternatives[] = {
        {"ssl.key.location", "ssl.key.pem"},
        {"ssl.certificate.location", "ssl.certificate.pem"},
        {"ssl.ca.location", "ssl.ca.pem"},
        {"https.ca.location", "https.ca.pem"},
        {"sasl.oauthbearer.assertion.private.key.file", "sasl.oauthbearer.assertion.private.key.pem"},
    };
    for (const auto & [path_property, pem_property] : pem_alternatives)
        if (name == path_property)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Kafka property `{}` can only be specified in a named collection defined in the server configuration file. "
                "Pass the contents of the file in the property `{}` instead",
                name,
                pem_property);

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Kafka property `{}` can only be specified in a named collection defined in the server configuration file",
        name);
}

void setKafkaConfigValue(cppkafka::Configuration & kafka_config, const String & key, const String & value, bool from_sql_collection)
{
    const String name = toKafkaPropertyName(key);
    if (from_sql_collection)
        checkSQLKafkaProperty(name, value);

    /// ClickHouse-specific keys that live under <kafka> in server config but are not librdkafka properties.
    /// Exclude them here so they are not forwarded to cppkafka.
    static const std::unordered_set<String> clickhouse_only_kafka_config_keys
        = {"use_environment_credentials"}; /// AWS MSK IAM: controls AWS credentials provider selection
    if (clickhouse_only_kafka_config_keys.contains(key))
        return;

    kafka_config.set(name, value);
}

void loadConfigProperty(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & tag)
{
    const String property_path = config_prefix + "." + tag;
    const String property_value = config.getString(property_path);

    setKafkaConfigValue(kafka_config, tag, property_value, /*from_sql_collection=*/false);
}

bool isNamedTopicSection(std::string_view tag)
{
    return tag == KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG || tag.starts_with("kafka_topic[");
}

/// Return the key within its configuration section. Dots in the property itself are significant.
std::string_view namedCollectionPropertyKey(std::string_view key)
{
    static constexpr std::string_view prefix = "kafka.";
    if (!key.starts_with(prefix))
        return {}; /// The collection has no properties below `kafka`.
    key.remove_prefix(prefix.size());
    if (key.starts_with("consumer.") || key.starts_with("producer."))
        key.remove_prefix(key.find('.') + 1);

    /// Both `kafka_<topic>` and `kafka_topic` (possibly indexed) are topic sections.
    if (const auto dot = key.find('.'); dot != std::string_view::npos && key.starts_with("kafka_"))
    {
        const auto section = key.substr(0, dot);
        key.remove_prefix(dot + 1);
        if (isNamedTopicSection(section) && key == KafkaConfigLoader::CONFIG_NAME_TAG)
            return {}; /// Topic selector, not a librdkafka property.
    }
    return key;
}

void loadNamedCollectionConfig(cppkafka::Configuration & kafka_config, const NamedCollection & collection, const String & config_prefix)
{
    for (const auto & key : collection.getKeys(-1, config_prefix))
    {
        const auto property_key = namedCollectionPropertyKey(key);
        if (property_key.empty() || key.size() != config_prefix.size() + 1 + property_key.size())
            continue; /// Subsections are loaded separately.
        const String param_name(property_key);
        const auto value = collection.get<String>(key);
        /// The check when the table is created or loaded is not sufficient: the collection is read again here,
        /// and `ALTER NAMED COLLECTION` can add a property while the table exists.
        setKafkaConfigValue(kafka_config, param_name, value, collection.getSourceId() != NamedCollection::SourceId::CONFIG);
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
        const auto collection = NamedCollectionFactory::instance().get(collection_name);
        /// For a topic called `topic`, the legacy section is also called `kafka_topic`.
        /// If it has a `name`, the topic selector must be matched by `loadFromConfig`.
        if (config_prefix == "kafka.kafka_topic" && collection->has(config_prefix + ".name"))
            return;
        loadNamedCollectionConfig(kafka_config, *collection, config_prefix);
        return;
    }

    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        loadConfigProperty(kafka_config, config, config_prefix, tag);
    }
}

/// Read a matching per-topic section of the server configuration.
void loadTopicConfig(cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);
    for (const auto & tag : tags)
    {
        if (tag != KafkaConfigLoader::CONFIG_NAME_TAG)
            loadConfigProperty(kafka_config, config, config_prefix, tag);
    }
}

/// Read server configuration into cppkafka configuration, used by global configuration and by legacy per-topic configuration
void loadFromConfig(
    cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix)
{
    if (!params.collection_name.empty())
    {
        const auto collection = NamedCollectionFactory::instance().get(params.collection_name);
        loadNamedCollectionConfig(kafka_config, *collection, config_prefix);
        for (const auto & key : collection->getKeys(0, config_prefix))
        {
            if (key.size() <= config_prefix.size() || !isNamedTopicSection(std::string_view(key).substr(config_prefix.size() + 1)))
                continue;
            const String name_key = key + "." + KafkaConfigLoader::CONFIG_NAME_TAG;
            if (collection->has(name_key) && std::ranges::contains(params.topics, collection->get<String>(name_key)))
                loadNamedCollectionConfig(kafka_config, *collection, key);
        }
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
                loadTopicConfig(kafka_config, params.config, kafka_topic_path);
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
        kafka_config.set("sasl.password", String(kafka_settings[KafkaSetting::kafka_sasl_password].value));
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
            const std::string safe_message = hideHexNumbers(message);
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
                safe_message);
            if (client_logs_level <= DB::LogsLevel::error)
            {
                if (auto sink_shared_ptr = sink.lock())
                {
                    ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
                    // librdkafka-originated errors (auth failures, broker disconnects) have no
                    // useful ClickHouse stack trace - the trace only shows poll->log_callback.
                    // Skip stack trace to reduce noise in system.kafka_consumers.exceptions.
                    sink_shared_ptr->setExceptionInfo(safe_message, /* with_stacktrace = */ false);
                }
            }
        });

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
    /// The properties librdkafka marks with the _RK_SENSITIVE flag or stores as pointers, plus a
    /// substring safety net for properties unknown to the vendored librdkafka version.
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

    if (params.enable_statistics_by_default)
        conf.set("statistics.interval.ms", "3000");

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
template cppkafka::Configuration
KafkaConfigLoader::getProducerConfiguration<StorageKafka2>(StorageKafka2 & storage, const ProducerConfigParams & params);

void KafkaConfigLoader::checkNamedCollection(const NamedCollection & collection)
{
    if (collection.getSourceId() == NamedCollection::SourceId::CONFIG)
        return;

    for (const auto & key : collection.getKeys(-1, CONFIG_KAFKA_TAG))
    {
        checkKafkaPropertyName(key);
        const auto property_key = namedCollectionPropertyKey(key);
        if (!property_key.empty())
            checkSQLKafkaProperty(toKafkaPropertyName(String(property_key)), collection.get<String>(key));
    }
}

}
