#pragma once

#include <Core/Names.h>
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
    static inline const String CONFIG_KAFKA_TAG = "kafka";
    static inline const String CONFIG_KAFKA_TOPIC_TAG = "kafka_topic";
    static inline const String CONFIG_NAME_TAG = "name";
    static inline const String CONFIG_KAFKA_CONSUMER_TAG = "consumer";
    static inline const String CONFIG_KAFKA_PRODUCER_TAG = "producer";

    static void loadConsumerConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const String & prefix,
        const Names & topics);

    static void loadProducerConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const String & prefix,
        const Names & topics);

    static void loadFromConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const String & config_prefix,
        const Names & topics);
};
}
