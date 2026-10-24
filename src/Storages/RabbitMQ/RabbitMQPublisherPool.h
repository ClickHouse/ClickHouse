#pragma once

#include <Storages/RabbitMQ/RabbitMQPublisher.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// Keeps idle publishers (a connection with an open channel) of one table, so that `INSERT` queries
/// do not pay for connecting to RabbitMQ every time.
class RabbitMQPublisherPool
{
public:
    RabbitMQPublisherPool(
        const RabbitMQConfiguration & configuration_,
        const Names & routing_keys_,
        const String & exchange_name_,
        AMQP::ExchangeType exchange_type_,
        bool persistent_,
        size_t max_idle_publishers_,
        LoggerPtr log_);

    /// Returns the most recently released publisher that is still usable, otherwise a new connected one.
    RabbitMQPublisherPtr acquire();

    /// Takes back a publisher with a usable channel and no pending deliveries.
    /// Closes it instead if the pool is full or shut down.
    void release(RabbitMQPublisherPtr publisher);

    void shutdown();

private:
    struct IdlePublisher
    {
        RabbitMQPublisherPtr publisher;
        std::chrono::steady_clock::time_point released_at;
    };

    /// Pops the most recently released publisher, disconnecting the expired ones.
    std::optional<IdlePublisher> popIdlePublisher();

    static bool isExpired(IdlePublisher & idle);

    const RabbitMQConfiguration configuration;
    const Names routing_keys;
    const String exchange_name;
    const AMQP::ExchangeType exchange_type;
    const bool persistent;
    const size_t max_idle_publishers;
    LoggerPtr log;

    /// Needed to generate unique channel ids.
    std::atomic<size_t> publisher_id = 1;

    std::mutex mutex;
    std::vector<IdlePublisher> idle_publishers;
    bool is_shutdown = false;
};

using RabbitMQPublisherPoolPtr = std::shared_ptr<RabbitMQPublisherPool>;

}
