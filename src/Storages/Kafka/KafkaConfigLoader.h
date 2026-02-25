#pragma once

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <Core/Names.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>

namespace DB
{
struct KafkaSettings;
class VirtualColumnsDescription;

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
}
