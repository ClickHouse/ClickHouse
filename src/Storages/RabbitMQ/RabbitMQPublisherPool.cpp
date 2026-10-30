#include <Storages/RabbitMQ/RabbitMQPublisherPool.h>

#include <Common/logger_useful.h>


namespace DB
{

RabbitMQPublisherPool::RabbitMQPublisherPool(
    const RabbitMQConfiguration & configuration_,
    const Names & routing_keys_,
    const String & exchange_name_,
    AMQP::ExchangeType exchange_type_,
    bool persistent_,
    size_t max_idle_publishers_,
    LoggerPtr log_)
    : configuration(configuration_)
    , routing_keys(routing_keys_)
    , exchange_name(exchange_name_)
    , exchange_type(exchange_type_)
    , persistent(persistent_)
    , max_idle_publishers(max_idle_publishers_)
    , log(log_)
{
}

RabbitMQPublisherPtr RabbitMQPublisherPool::acquire()
{
    /// The network is only touched outside of the mutex.
    while (auto idle = popIdlePublisher())
    {
        if (idle->publisher->isReusable())
        {
            LOG_TRACE(log, "Reusing an idle producer connection");
            return std::move(idle->publisher);
        }

        LOG_DEBUG(log, "Discarding an idle producer connection, which is no longer usable");
        idle->publisher->disconnect(/* immediately */ true);
    }

    auto publisher = std::make_unique<RabbitMQPublisher>(
        configuration, routing_keys, exchange_name, exchange_type, publisher_id.fetch_add(1), persistent, log);
    publisher->connect();
    return publisher;
}

void RabbitMQPublisherPool::release(RabbitMQPublisherPtr publisher)
{
    {
        std::lock_guard lock(mutex);
        if (!is_shutdown && idle_publishers.size() < max_idle_publishers)
        {
            idle_publishers.push_back({std::move(publisher), std::chrono::steady_clock::now()});
            return;
        }
    }

    publisher->closeChannel();
    publisher->disconnect();
}

void RabbitMQPublisherPool::shutdown()
{
    std::vector<IdlePublisher> publishers_to_disconnect;
    {
        std::lock_guard lock(mutex);
        is_shutdown = true;
        publishers_to_disconnect.swap(idle_publishers);
    }

    /// Idle publishers have no pending deliveries, so closing the connection does not lose messages.
    for (auto & idle : publishers_to_disconnect)
        idle.publisher->disconnect();
}

std::optional<RabbitMQPublisherPool::IdlePublisher> RabbitMQPublisherPool::popIdlePublisher()
{
    std::vector<IdlePublisher> expired_publishers;
    std::optional<IdlePublisher> idle;
    {
        std::lock_guard lock(mutex);

        /// Only the most recently released publisher is taken, so the older ones are checked here,
        /// otherwise their closed connections would be kept until the table is shut down.
        std::erase_if(idle_publishers, [&](IdlePublisher & publisher)
        {
            if (!isExpired(publisher))
                return false;
            expired_publishers.push_back(std::move(publisher));
            return true;
        });

        if (!idle_publishers.empty())
        {
            idle = std::move(idle_publishers.back());
            idle_publishers.pop_back();
        }
    }

    if (!expired_publishers.empty())
        LOG_DEBUG(log, "Discarding {} expired idle producer connections", expired_publishers.size());
    for (auto & expired : expired_publishers)
        expired.publisher->disconnect(/* immediately */ true);

    return idle;
}

bool RabbitMQPublisherPool::isExpired(IdlePublisher & idle)
{
    const auto heartbeat_interval = idle.publisher->getHeartbeatInterval();
    if (heartbeat_interval == 0)
        return false;

    /// The server closes the connection after the full interval without receiving anything. Half of it leaves
    /// a margin for the insert itself, and server heartbeats are sent about twice per interval, which keeps
    /// intermediate network devices from silently dropping the connection.
    return 2 * (std::chrono::steady_clock::now() - idle.released_at) >= std::chrono::seconds(heartbeat_interval);
}

}
