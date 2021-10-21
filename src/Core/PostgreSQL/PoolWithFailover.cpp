#include "PoolWithFailover.h"

#if USE_LIBPQXX

#include "Utils.h"
#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>

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
        const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
        size_t pool_size, size_t pool_wait_timeout_, size_t max_tries_)
        : pool_wait_timeout(pool_wait_timeout_)
        , max_tries(max_tries_)
{
    LOG_TRACE(&Poco::Logger::get("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    auto db = config.getString(config_prefix + ".db", "");
    auto host = config.getString(config_prefix + ".host", "");
    auto port = config.getUInt(config_prefix + ".port", 0);
    auto user = config.getString(config_prefix + ".user", "");
    auto password = config.getString(config_prefix + ".password", "");

    if (config.has(config_prefix + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_prefix, config_keys);

        for (const auto & config_key : config_keys)
        {
            if (config_key.starts_with("replica"))
            {
                std::string replica_name = config_prefix + "." + config_key;
                size_t priority = config.getInt(replica_name + ".priority", 0);

                auto replica_host = config.getString(replica_name + ".host", host);
                auto replica_port = config.getUInt(replica_name + ".port", port);
                auto replica_user = config.getString(replica_name + ".user", user);
                auto replica_password = config.getString(replica_name + ".password", password);

                auto connection_string = formatConnectionString(db, replica_host, replica_port, replica_user, replica_password).first;
                replicas_with_priority[priority].emplace_back(connection_string, pool_size);
            }
        }
    }
    else
    {
        auto connection_string = formatConnectionString(db, host, port, user, password).first;
        replicas_with_priority[0].emplace_back(connection_string, pool_size);
    }
}

PoolWithFailover::PoolWithFailover(
        const std::string & database,
        const RemoteDescription & addresses,
        const std::string & user, const std::string & password,
        size_t pool_size, size_t pool_wait_timeout_, size_t max_tries_)
    : pool_wait_timeout(pool_wait_timeout_)
    , max_tries(max_tries_)
{
    LOG_TRACE(&Poco::Logger::get("PostgreSQLConnectionPool"), "PostgreSQL connection pool size: {}, connection wait timeout: {}, max failover tries: {}",
              pool_size, pool_wait_timeout, max_tries_);

    /// Replicas have the same priority, but traversed replicas are moved to the end of the queue.
    for (const auto & [host, port] : addresses)
    {
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLPoolWithFailover"), "Adding address host: {}, port: {} to connection pool", host, port);
        auto connection_string = formatConnectionString(database, host, port, user, password).first;
        replicas_with_priority[0].emplace_back(connection_string, pool_size);
    }
}

ConnectionHolderPtr PoolWithFailover::get()
{
    std::lock_guard lock(mutex);

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
                    if (!connection || !connection->is_open())
                    {
                        connection = std::make_unique<pqxx::connection>(replica.connection_string);
                        LOG_DEBUG(log, "New connection to {}:{}", connection->hostname(), connection->port());
                    }
                }
                catch (const pqxx::broken_connection & pqxx_error)
                {
                    LOG_ERROR(log, "Connection error: {}", pqxx_error.what());

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

    throw DB::Exception(DB::ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "Unable to connect to any of the replicas");
}
}

#endif
