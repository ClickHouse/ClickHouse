#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <Storages/Pulsar/Client/Configuration.h>
#include <Storages/Pulsar/Client/Message.h>
#include <Storages/Pulsar/Client/Result.h>
#include <Common/Logger.h>

namespace DB::Pulsar
{

class ProducerImpl;
class ConnectionRegistry;

/// A handle to a producer on one topic. Default-constructed empty and filled by `Client::createProducer`.
/// Copies share the same producer. All calls are synchronous.
class Producer
{
public:
    Producer();
    ~Producer();
    Producer(const Producer &);
    Producer & operator=(const Producer &);

    /// Opens a connection to `host:port` (the broker that owns `topic`, as resolved by lookup), registers
    /// the producer there, and returns a ready handle. On failure returns an empty handle and sets `result`.
    static Producer open(
        const std::string & host,
        uint16_t port,
        const ClientConfiguration & client_config,
        const std::string & topic,
        const ProducerConfiguration & producer_config,
        ConnectionRegistry & registry,
        const LoggerPtr & log,
        Result & result);

    /// Publishes one message and waits for the broker receipt.
    Result send(const Message & message);
    /// No-op: `send` is synchronous, so nothing is ever pending. Kept for API parity.
    Result flush();
    /// Unregisters the producer on the broker and closes its connection.
    Result close();

private:
    explicit Producer(std::shared_ptr<ProducerImpl> impl_);
    std::shared_ptr<ProducerImpl> impl;
};

}
