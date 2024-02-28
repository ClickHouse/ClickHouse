#pragma once

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <librdkafka/rdkafka.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{
template <typename TKafkaStorage>
struct StorageKafkaInterceptors
{
    static rd_kafka_resp_err_t rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx);

    static rd_kafka_resp_err_t rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx);

    static rd_kafka_resp_err_t
    rdKafkaOnNew(rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/);

    static rd_kafka_resp_err_t rdKafkaOnConfDup(
        rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx);
};

struct KafkaConfigLoader
{
    static constexpr std::string_view CONFIG_KAFKA_TAG = "kafka";
    static constexpr std::string_view CONFIG_KAFKA_TOPIC_TAG = "kafka_topic";
    static constexpr std::string_view CONFIG_NAME_TAG = "name";

    /// Read server configuration into cppkafka configuration, used by global configuration and by legacy per-topic configuration
    static void loadConfig(
        cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

    /// Read server configuration into cppkafa configuration, used by new per-topic configuration
    static void loadTopicConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const String & topic);
};
}
