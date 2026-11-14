#include <Storages/RabbitMQ/RabbitMQPublisherPool.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <iterator>


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
    while (auto idle = popIdlePublisher())
    {
        if (!isExpired(*idle) && idle->publisher->isReusable())
        {
            LOG_TRACE(log, "Reusing an idle producer connection");
            return std::move(idle->publisher);
        }

        LOG_DEBUG(log, "Discarding an idle producer connection, which is expired or no longer usable");
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

void RabbitMQPublisherPool::cleanupIdlePublishers()
{
    std::vector<IdlePublisher> publishers_to_check;
    {
        std::lock_guard lock(mutex);
        publishers_to_check.swap(idle_publishers);
    }

    std::vector<IdlePublisher> usable_publishers;
    size_t num_discarded = 0;
    for (auto & idle : publishers_to_check)
    {
        if (isExpired(idle) || !idle.publisher->isReusable())
        {
            idle.publisher->disconnect(/* immediately */ true);
            ++num_discarded;
        }
        else
            usable_publishers.push_back(std::move(idle));
    }

    if (num_discarded)
        LOG_DEBUG(log, "Discarded {} idle producer connections, which are expired or no longer usable", num_discarded);

    std::vector<IdlePublisher> excess_publishers;
    {
        std::lock_guard lock(mutex);
        if (is_shutdown)
        {
            excess_publishers.swap(usable_publishers);
        }
        else
        {
            /// Publishers released in the meantime are more recent, so the checked ones go to the front.
            idle_publishers.insert(
                idle_publishers.begin(), std::make_move_iterator(usable_publishers.begin()), std::make_move_iterator(usable_publishers.end()));

            /// The released ones may have filled the pool while it was being checked, then the oldest ones are closed.
            if (idle_publishers.size() > max_idle_publishers)
            {
                const auto num_excess = idle_publishers.size() - max_idle_publishers;
                std::move(idle_publishers.begin(), idle_publishers.begin() + num_excess, std::back_inserter(excess_publishers));
                idle_publishers.erase(idle_publishers.begin(), idle_publishers.begin() + num_excess);
            }
        }
    }

    for (auto & excess : excess_publishers)
        excess.publisher->disconnect();
}

std::optional<RabbitMQPublisherPool::IdlePublisher> RabbitMQPublisherPool::popIdlePublisher()
{
    std::lock_guard lock(mutex);
    if (idle_publishers.empty())
        return {};

    auto idle = std::move(idle_publishers.back());
    idle_publishers.pop_back();
    return idle;
}

bool RabbitMQPublisherPool::isExpired(const IdlePublisher & idle)
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
