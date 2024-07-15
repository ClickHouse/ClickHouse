#pragma once

#include <chrono>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>
#include <cppkafka/configuration.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <fmt/ostream.h>
#include <Core/SettingsEnums.h>
#include <librdkafka/rdkafka.h>
#include <Common/SettingsChanges.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

struct KafkaSettings;
class VirtualColumnsDescription;

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
    using LogCallback = cppkafka::Configuration::LogCallback;


    struct LoadConfigParams
    {
        const Poco::Util::AbstractConfiguration & config;
        String & collection_name;
        const Names & topics;
        LoggerPtr & log;
    };

    struct ConsumerConfigParams : public LoadConfigParams
    {
        String brokers;
        String group;
        bool multiple_consumers;
        size_t consumer_number;
        String client_id;
        size_t max_block_size;
    };

    struct ProducerConfigParams : public LoadConfigParams
    {
        String brokers;
        String client_id;
    };

    template <typename TKafkaStorage>
    static cppkafka::Configuration getConsumerConfiguration(TKafkaStorage & storage, const ConsumerConfigParams & params);

    template <typename TKafkaStorage>
    static cppkafka::Configuration getProducerConfiguration(TKafkaStorage & storage, const ProducerConfigParams & params);
};

namespace StorageKafkaUtils
{
Names parseTopics(String topic_list);
String getDefaultClientId(const StorageID & table_id);

using ErrorHandler = std::function<void(const cppkafka::Error &)>;

void drainConsumer(
    cppkafka::Consumer & consumer,
    std::chrono::milliseconds drain_timeout,
    const LoggerPtr & log,
    ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

using Messages = std::vector<cppkafka::Message>;
void eraseMessageErrors(Messages & messages, const LoggerPtr & log, ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

SettingsChanges createSettingsAdjustments(KafkaSettings & kafka_settings, const String & schema_name);

bool checkDependencies(const StorageID & table_id, const ContextPtr& context);

VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode);
}
}

template <>
struct fmt::formatter<cppkafka::TopicPartition> : fmt::ostream_formatter
{
};
template <>
struct fmt::formatter<cppkafka::Error> : fmt::ostream_formatter
{
};
