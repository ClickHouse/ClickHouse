#pragma once

#include "config.h"

#if USE_PULSAR

#include <base/types.h>

#include <chrono>
#include <functional>
#include <memory>
#include <vector>


/// A client of the Pulsar binary protocol implementing what the `Pulsar` table engine needs: shared subscriptions
/// to (partitioned) topics with batched, chunked and compressed messages, acknowledgements, and a producer that sends
/// one message at a time and waits for its receipt. TLS uses the `openSSL.client` configuration of the server.
///
/// The client runs no threads: all network I/O happens inside the calls, which wait only up to their timeouts, apart from DNS resolution.
/// A consumer or producer is used by one thread at a time; only `Client::close` and `Producer::interrupt` may be called
/// from another thread. After a call has thrown, the consumer or producer must be closed and created again.
namespace DB::Pulsar
{

class Consumer;
class Producer;
struct ClientState;

struct ClientConfiguration
{
    /// Called with the host and port, as in the service URL or in a lookup response, before every connection
    /// to them. Throws to reject the connection.
    std::function<void(const String & host, UInt16 port)> connection_validator;
    std::chrono::milliseconds connection_timeout{10'000};
    /// For every request to a broker, and for `Client::subscribe` and `Client::createProducer` as a whole.
    std::chrono::milliseconds operation_timeout{30'000};
    /// A connection that received nothing for this long is checked with `PING`, and is lost if the `PONG`
    /// does not come within the same time.
    std::chrono::milliseconds keep_alive_interval{30'000};
};

enum class InitialPosition : uint8_t
{
    Latest,
    Earliest,
};

struct ConsumerConfiguration
{
    InitialPosition initial_position = InitialPosition::Latest;
    /// The number of messages to prefetch from one topic partition.
    size_t receiver_queue_size = 1000;
    /// How often the number of partitions of the partitioned topics is checked to subscribe to new partitions.
    std::chrono::milliseconds partitions_refresh_interval{60'000};
};

struct ProducerConfiguration
{
    std::chrono::milliseconds send_timeout{30'000};
};

class Client
{
public:
    /// `pulsar://host[:port][,host[:port]...]` or `pulsar+ssl://...`.
    Client(const String & service_url, ClientConfiguration configuration);
    ~Client();

    /// Subscribes to all partitions of the topics with a `Shared` subscription.
    std::unique_ptr<Consumer> subscribe(const std::vector<String> & topics, const String & subscription, const ConsumerConfiguration & configuration); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// For a partitioned topic, the producer writes to one random partition.
    std::unique_ptr<Producer> createProducer(const String & topic, const ProducerConfiguration & configuration);

    /// Thread-safe. The calls in progress on all consumers and producers throw `ABORTED`, as all later calls do.
    void close() noexcept;

private:
    std::shared_ptr<ClientState> state;
};

}

#endif
