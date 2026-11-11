#pragma once

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <Core/Names.h>
#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>

namespace DB
{
struct KafkaSettings;
class VirtualColumnsDescription;
class StorageKafka2;

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
        ContextPtr context;
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
    static cppkafka::Configuration getConsumerConfiguration(TKafkaStorage & storage, const ConsumerConfigParams & params, IKafkaExceptionInfoSinkPtr exception_info_sink_ptr = IKafkaExceptionInfoSinkPtr());

    template <typename TKafkaStorage>
    static cppkafka::Configuration getProducerConfiguration(TKafkaStorage & storage, const ProducerConfigParams & params);

    /// --- The Kafka2 engine (the in-tree client) ---

    struct Kafka2ConfigParams : public LoadConfigParams
    {
        String brokers;
        String client_id;
        bool multiple_consumers = false;
        size_t consumer_number = 0;
    };

    /// One unified client configuration serves both consumption and production;
    /// the producer-only compression knobs ride along separately.
    struct Kafka2Configuration
    {
        Kafka::ClientConfig client;
        Kafka::Compression compression = Kafka::Compression::None;
        Int64 compression_level = -1;
    };

    /// Builds the in-tree client configuration from the table settings and the
    /// server configuration. Only a small allowlist of librdkafka-style
    /// properties is meaningful for the in-tree client; every other property
    /// found in the `<kafka>` sections is logged and ignored.
    static Kafka2Configuration getKafka2Configuration(StorageKafka2 & storage, const Kafka2ConfigParams & params);
};
}
