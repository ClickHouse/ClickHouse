#pragma once

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>

namespace DB
{
struct KafkaSettings;
class NamedCollection;
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
        /// The named collection of the table with the overrides from its definition, or `nullptr`.
        const NamedCollectionPtr & collection;
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

    /// Sensitive properties must not be shown in cleartext: neither in the logs, whose records can reach
    /// clients that set `send_logs_level`, nor in the table definition.
    static bool isSensitiveProperty(std::string_view name);

    /// Throws if a named collection created with SQL sets a librdkafka property that is not explicitly allowed.
    /// The same check is done when the configuration of a consumer or a producer is loaded, because the named
    /// collection can change while the table exists.
    static void checkNamedCollection(const NamedCollection & collection);
};
}
