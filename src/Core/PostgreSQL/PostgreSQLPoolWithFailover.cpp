#include "PostgreSQLPoolWithFailover.h"
#include "PostgreSQLConnection.h"
#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>


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
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const size_t max_tries_)
        : max_tries(max_tries_)
{
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

                replicas_with_priority[priority].emplace_back(std::make_shared<ConnectionPool>(db, replica_host, replica_port, replica_user, replica_password));
            }
        }
    }
    else
    {
        replicas_with_priority[0].emplace_back(std::make_shared<ConnectionPool>(db, host, port, user, password));
    }
}


PoolWithFailover::PoolWithFailover(
        const std::string & database,
        const RemoteDescription & addresses,
        const std::string & user,
        const std::string & password,
        size_t pool_size,
        int64_t pool_wait_timeout,
        size_t max_tries_)
    : max_tries(max_tries_)
{
    /// Replicas have the same priority, but traversed replicas are moved to the end of the queue.
    for (const auto & [host, port] : addresses)
    {
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLPoolWithFailover"), "Adding address host: {}, port: {} to connection pool", host, port);
        replicas_with_priority[0].emplace_back(std::make_shared<ConnectionPool>(database, host, port, user, password, pool_size, pool_wait_timeout));
    }
}


PoolWithFailover::PoolWithFailover(const PoolWithFailover & other)
        : replicas_with_priority(other.replicas_with_priority)
        , max_tries(other.max_tries)
{
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
                auto connection = replicas[i]->get();
                if (connection->isConnected())
                {
                    /// Move all traversed replicas to the end.
                    std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());
                    return connection;
                }
            }
        }
    }

    throw DB::Exception(DB::ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "Unable to connect to any of the replicas");
}

}
