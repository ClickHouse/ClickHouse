#include "PoolWithFailover.h"

#include <random>
#include <thread>

namespace mysqlxx
{

std::shared_ptr<PoolWithFailover> PoolWithFailover::create(const ReplicasConfigurations & pools_configuration)
{
    std::unique_ptr<PoolWithFailover> pool(new PoolWithFailover(pools_configuration));
    std::shared_ptr<PoolWithFailover> pool_ptr(std::move(pool));

    return pool_ptr;
}

PoolWithFailover::PoolWithFailover(const ReplicasConfigurations & pools_configuration)
    : logger(Poco::Logger::get("mysqlxx::PoolWithFailover"))
{
    for (const auto & replica_configuration : pools_configuration)
    {
        size_t priority = replica_configuration.priority;
        auto pool = Pool::create(replica_configuration.connection_configuration, replica_configuration.pool_configuration);
        priority_to_replicas[priority].emplace_back(std::move(pool));
    }

    static thread_local std::mt19937 rnd_generator(std::hash<std::thread::id>{}(std::this_thread::get_id()) + std::clock());
    for (auto & [_, replicas] : priority_to_replicas)
    {
        if (replicas.size() > 1)
            std::shuffle(replicas.begin(), replicas.end(), rnd_generator);
    }
}

IPool::Entry PoolWithFailover::getEntry()
{
    /// TODO: Max try size
    for (auto & [_, replicas] : priority_to_replicas)
    {
        for (size_t i = 0, size = replicas.size(); i < size; ++i)
        {
            PoolPtr & pool = replicas[i];

            try
            {
                Entry entry = pool->getEntry();

                {
                    std::lock_guard<std::mutex> lock(mutex);
                    /// Move all traversed replicas to the end of queue.
                    /// (No need to move replicas with another priority)
                    std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());
                }

                return entry;
            }
            catch (const Poco::Exception & e)
            {
                logger.warning("Connection to " + pool->getConnectionConfiguration().getDescription() + " failed: " + e.displayText());
                continue;
            }
        }
    }

    std::stringstream message;
    message << "Connections to all replicas failed: ";
    for (auto it = priority_to_replicas.begin(); it != priority_to_replicas.end(); ++it)
        for (auto jt = it->second.begin(); jt != it->second.end(); ++jt)
            message << (it == priority_to_replicas.begin() && jt == it->second.begin() ? "" : ", ") << (*jt)->getConnectionConfiguration().getDescription();

    throw Poco::Exception(message.str());
}

IPool::Entry PoolWithFailover::tryGetEntry(size_t timeout_in_milliseconds)
{
    /// TODO: Max try size
    auto try_get_entry_start = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

    for (auto & [_, replicas] : priority_to_replicas)
    {
        for (size_t i = 0, size = replicas.size(); i < size; ++i)
        {
            PoolPtr & pool = replicas[i];

            Entry entry = pool->tryGetEntry(timeout_in_milliseconds);

            if (entry.isNull())
            {
                auto time_after_replica_get = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
                auto time_spend = time_after_replica_get - try_get_entry_start;
                try_get_entry_start = time_after_replica_get;

                size_t time_spend_milliseconds = time_spend.count();
                if (time_spend_milliseconds >= timeout_in_milliseconds)
                    return IPool::Entry();
                else
                    timeout_in_milliseconds -= time_spend_milliseconds;

                logger.warning("Connection to " + pool->getConnectionConfiguration().getDescription() + " failed");
                continue;
            }
            else
            {
                std::lock_guard<std::mutex> lock(mutex);
                /// Move all traversed replicas to the end of queue.
                /// (No need to move replicas with another priority)
                std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());
            }
        }
    }

    std::stringstream message;
    message << "Connections to all replicas failed: ";
    for (auto it = priority_to_replicas.begin(); it != priority_to_replicas.end(); ++it)
        for (auto jt = it->second.begin(); jt != it->second.end(); ++jt)
            message << (it == priority_to_replicas.begin() && jt == it->second.begin() ? "" : ", ") << (*jt)->getConnectionConfiguration().getDescription();

    return IPool::Entry();
}

void PoolWithFailover::returnConnectionToPool(mysqlxx::Connection &&)
{
}

}
