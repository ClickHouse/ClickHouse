#include <Core/PostgreSQL/PoolWithFailover.h>
#include <memory>

#if USE_LIBPQXX

#include <Core/PostgreSQL/Utils.h>
#include <Common/ReplicasReconnector.h>
#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int POSTGRESQL_CONNECTION_FAILURE;
    extern const int LOGICAL_ERROR;
}
}

namespace postgres
{

auto PoolWithFailover::connectionReestablisher(std::weak_ptr<PoolHolder> pool, size_t pool_wait_timeout)
{
    return [weak_pool = pool, pool_wait_timeout](UInt64 interval_milliseconds)
    {
        auto shared_pool = weak_pool.lock();
        if (!shared_pool)
            return false;

        if (!shared_pool->online)
        {
            auto logger = getLogger("PostgreSQLConnectionPool");

            ConnectionPtr connection;
            auto connection_available = shared_pool->pool->tryBorrowObject(connection, []() { return nullptr; }, pool_wait_timeout);

            if (!connection_available)
            {
                LOG_WARNING(logger, "Reestablishing connection to {} has failed: unable to fetch connection within the timeout.", connection->getInfoForLog());
                return true;
            }

            try
            {
                /// Create a new connection or reopen an old connection if it became invalid.
                if (!connection)
                    connection = std::make_unique<Connection>(shared_pool->connection_info);

                connection->connect();
                shared_pool->online = true;
                LOG_INFO(logger, "Reestablishing connection to {} has succeeded.", connection->getInfoForLog());
            }
            catch (const pqxx::broken_connection & pqxx_error)
            {
                if (interval_milliseconds >= 1000)
                    LOG_WARNING(logger, "Reestablishing connection to {} has failed: {}", connection->getInfoForLog(), pqxx_error.what());
                shared_pool->online = false;
                shared_pool->pool->returnObject(std::move(connection));
            }
            catch (const Poco::Exception & e)
            {
                if (interval_milliseconds >= 1000)
                    LOG_WARNING(logger, "Reestablishing connection to {} has failed: {}", connection->getInfoForLog(), e.displayText());
                shared_pool->online = false;
                shared_pool->pool->returnObject(std::move(connection));
            }
            catch (...)
            {
                if (interval_milliseconds >= 1000)
                    LOG_WARNING(logger, "Reestablishing connection to {} has failed.", connection->getInfoForLog());
                shared_pool->online = false;
                shared_pool->pool->returnObject(std::move(connection));
            }
        }

        return true;
    };
}

PoolWithFailover::PoolWithFailover(
    const ReplicasConfigurationByPriority & configurations_by_priority,
    size_t pool_size,
    size_t pool_wait_timeout_,
    size_t max_tries_,
    bool auto_close_connection_,
    size_t connection_attempt_timeout_,
    bool bg_reconnect_)
    : pool_wait_timeout(pool_wait_timeout_)
    , max_tries(max_tries_)
    , auto_close_connection(auto_close_connection_)
    , bg_reconnect(bg_reconnect_)
{
    LOG_TRACE(getLogger("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    for (const auto & [priority, configurations] : configurations_by_priority)
    {
        for (const auto & replica_configuration : configurations)
        {
            auto connection_info = formatConnectionString(
                replica_configuration.database,
                replica_configuration.host,
                replica_configuration.port,
                replica_configuration.username,
                replica_configuration.password,
                connection_attempt_timeout_);
            replicas_with_priority[priority].emplace_back(std::make_shared<PoolHolder>(connection_info, pool_size));
            if (bg_reconnect)
                DB::ReplicasReconnector::instance().add(connectionReestablisher(std::weak_ptr(replicas_with_priority[priority].back()), pool_wait_timeout));
        }
    }
}

PoolWithFailover::PoolWithFailover(
    const DB::StoragePostgreSQL::Configuration & configuration,
    size_t pool_size,
    size_t pool_wait_timeout_,
    size_t max_tries_,
    bool auto_close_connection_,
    size_t connection_attempt_timeout_,
    bool bg_reconnect_)
    : pool_wait_timeout(pool_wait_timeout_)
    , max_tries(max_tries_)
    , auto_close_connection(auto_close_connection_)
    , bg_reconnect(bg_reconnect_)
{
    LOG_TRACE(getLogger("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    /// Replicas have the same priority, but traversed replicas are moved to the end of the queue.
    for (const auto & [host, port] : configuration.addresses)
    {
        LOG_DEBUG(getLogger("PostgreSQLPoolWithFailover"), "Adding address host: {}, port: {} to connection pool", host, port);
        auto connection_string = formatConnectionString(
            configuration.database,
            host,
            port,
            configuration.username,
            configuration.password,
            connection_attempt_timeout_);
        replicas_with_priority[0].emplace_back(std::make_shared<PoolHolder>(connection_string, pool_size));
        if (bg_reconnect)
            DB::ReplicasReconnector::instance().add(connectionReestablisher(std::weak_ptr(replicas_with_priority[0].back()), pool_wait_timeout));
    }
}

ConnectionHolderPtr PoolWithFailover::get()
{
    std::lock_guard lock(mutex);

    if (replicas_with_priority.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No address specified");

    PreformattedMessage error_message;
    for (size_t try_idx = 0; try_idx < max_tries; ++try_idx)
    {
        for (auto & priority : replicas_with_priority)
        {
            auto & replicas = priority.second;
            for (size_t i = 0; i < replicas.size(); ++i)
            {
                auto & replica = replicas[i];

                if (bg_reconnect && !replica->online)
                    continue;

                ConnectionPtr connection;
                auto connection_available = replica->pool->tryBorrowObject(connection, []() { return nullptr; }, pool_wait_timeout);

                if (!connection_available)
                {
                    LOG_WARNING(log, "Unable to fetch connection within the timeout");
                    continue;
                }

                try
                {
                    /// Create a new connection or reopen an old connection if it became invalid.
                    if (!connection)
                    {
                        connection = std::make_unique<Connection>(replica->connection_info);
                        LOG_DEBUG(log, "New connection to {}", connection->getInfoForLog());
                    }

                    connection->connect();
                    replica->online = true;
                }
                catch (const pqxx::broken_connection & pqxx_error)
                {
                    LOG_ERROR(log, "Connection error: {}", pqxx_error.what());
                    error_message = PreformattedMessage::create(
                        "Try {}. Connection to {} failed with error: {}\n",
                        try_idx + 1, DB::backQuote(replica->connection_info.host_port), pqxx_error.what());

                    replica->online = false;
                    replica->pool->returnObject(std::move(connection));
                    continue;
                }
                catch (...)
                {
                    replica->online = false;
                    replica->pool->returnObject(std::move(connection));
                    throw;
                }

                auto connection_holder = std::make_unique<ConnectionHolder>(replica->pool, std::move(connection), auto_close_connection);

                /// Move all traversed replicas to the end.
                if (replicas.size() > 1)
                    std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());

                return connection_holder;
            }
        }
    }

    throw DB::Exception(error_message, DB::ErrorCodes::POSTGRESQL_CONNECTION_FAILURE);
}
}

#endif
