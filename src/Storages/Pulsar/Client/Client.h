#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <Storages/Pulsar/Client/Configuration.h>
#include <Storages/Pulsar/Client/ConnectionRegistry.h>
#include <Storages/Pulsar/Client/Consumer.h>
#include <Storages/Pulsar/Client/Producer.h>
#include <Storages/Pulsar/Client/Result.h>
#include <Common/Logger.h>

namespace DB::Pulsar
{

/// A minimal Apache Pulsar client that speaks only the binary protocol (`pulsar://`).
///
/// It owns no background threads and no event loop. Producers and consumers it hands out each hold
/// their own synchronous connection; `close` shuts them all down. The service URL may list several
/// brokers (`pulsar://host1:6650,host2:6650`); any of them is used for topic lookup.
///
/// Not supported (and reported as an error rather than silently mishandled): TLS (`pulsar+ssl://`),
/// authentication, and partitioned topics.
class Client
{
public:
    Client(const std::string & service_url, const ClientConfiguration & config);

    Client(const Client &) = delete;
    Client & operator=(const Client &) = delete;

    /// Creates a producer on `topic`. `topic` must be a single, non-partitioned topic.
    Result createProducer(const std::string & topic, const ProducerConfiguration & config, Producer & producer);

    /// Subscribes to every topic in `topics` under `subscription` as a shared consumer.
    Result subscribe(
        const std::vector<std::string> & topics,
        const std::string & subscription,
        const ConsumerConfiguration & config,
        Consumer & consumer);

    /// Closes every connection opened through this client, unblocking consumers parked in `batchReceive`.
    void close();

private:
    struct BrokerAddress
    {
        std::string host;
        uint16_t port;
    };

    /// Resolves `topic` to the broker that owns it, following lookup redirects. Fails for a partitioned
    /// topic (not supported).
    Result lookup(const std::string & topic, BrokerAddress & broker);

    ClientConfiguration config;
    LoggerPtr log;
    std::vector<BrokerAddress> service_brokers;
    bool tls_requested = false;
    ConnectionRegistry registry;
};

}
