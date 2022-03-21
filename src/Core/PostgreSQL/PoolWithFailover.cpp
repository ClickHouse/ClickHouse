#include "PoolWithFailover.h"

#if USE_LIBPQXX

#include "Utils.h"
#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int POSTGRESQL_CONNECTION_FAILURE;
}
}

namespace postgres
{

PoolWithFailover::PoolWithFailover(
        const DB::ExternalDataSourcesConfigurationByPriority & configurations_by_priority,
        size_t pool_size, size_t pool_wait_timeout_, size_t max_tries_)
        : pool_wait_timeout(pool_wait_timeout_)
        , max_tries(max_tries_)
{
    LOG_TRACE(&Poco::Logger::get("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    for (const auto & [priority, configurations] : configurations_by_priority)
    {
        for (const auto & replica_configuration : configurations)
        {
            auto connection_info = formatConnectionString(replica_configuration.database,
                replica_configuration.host, replica_configuration.port, replica_configuration.username, replica_configuration.password);
            replicas_with_priority[priority].emplace_back(connection_info, pool_size);
        }
    }
}

PoolWithFailover::PoolWithFailover(
        const DB::StoragePostgreSQLConfiguration & configuration,
        size_t pool_size, size_t pool_wait_timeout_, size_t max_tries_)
    : pool_wait_timeout(pool_wait_timeout_)
    , max_tries(max_tries_)
{
    LOG_TRACE(&Poco::Logger::get("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    /// Replicas have the same priority, but traversed replicas are moved to the end of the queue.
    for (const auto & [host, port] : configuration.addresses)
    {
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLPoolWithFailover"), "Adding address host: {}, port: {} to connection pool", host, port);
        auto connection_string = formatConnectionString(configuration.database, host, port, configuration.username, configuration.password);
        replicas_with_priority[0].emplace_back(connection_string, pool_size);
    }
}

ConnectionHolderPtr PoolWithFailover::get()
{
    std::lock_guard lock(mutex);

    DB::WriteBufferFromOwnString error_message;
    for (size_t try_idx = 0; try_idx < max_tries; ++try_idx)
    {
        for (auto & priority : replicas_with_priority)
        {
            auto & replicas = priority.second;
            for (size_t i = 0; i < replicas.size(); ++i)
            {
                auto & replica = replicas[i];

                ConnectionPtr connection;
                auto connection_available = replica.pool->tryBorrowObject(connection, []() { return nullptr; }, pool_wait_timeout);

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
                        connection = std::make_unique<Connection>(replica.connection_info);
                        LOG_DEBUG(log, "New connection to {}", connection->getInfoForLog());
                    }

                    connection->connect();
                }
                catch (const pqxx::broken_connection & pqxx_error)
                {
                    LOG_ERROR(log, "Connection error: {}", pqxx_error.what());
                    error_message << "Try " << try_idx + 1 << ". Connection to `" << replica.connection_info.host_port << "` failed: " << pqxx_error.what() << "\n";

                    replica.pool->returnObject(std::move(connection));
                    continue;
                }
                catch (...)
                {
                    replica.pool->returnObject(std::move(connection));
                    throw;
                }

                auto connection_holder = std::make_unique<ConnectionHolder>(replica.pool, std::move(connection));

                /// Move all traversed replicas to the end.
                if (replicas.size() > 1)
                    std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());

                return connection_holder;
            }
        }
    }

    throw DB::Exception(DB::ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, error_message.str());
}
}

#endif
