#include "PostgreSQLReplicaConnection.h"
#include "PostgreSQLConnection.h"
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int POSTGRESQL_CONNECTION_FAILURE;
}


PostgreSQLReplicaConnection::PostgreSQLReplicaConnection(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const size_t num_retries_)
        : num_retries(num_retries_)
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

                replicas[priority] = std::make_shared<PostgreSQLConnectionPool>(db, replica_host, replica_port, replica_user, replica_password);
            }
        }
    }
    else
    {
        replicas[0] = std::make_shared<PostgreSQLConnectionPool>(db, host, port, user, password);
    }
}


PostgreSQLReplicaConnection::PostgreSQLReplicaConnection(const PostgreSQLReplicaConnection & other)
        : replicas(other.replicas)
        , num_retries(other.num_retries)
{
}


PostgreSQLConnectionHolderPtr PostgreSQLReplicaConnection::get()
{
    std::lock_guard lock(mutex);

    for (size_t i = 0; i < num_retries; ++i)
    {
        for (auto & replica : replicas)
        {
            auto connection = replica.second->get();
            if (connection->isConnected())
                return connection;
        }
    }

    throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "Unable to connect to any of the replicas");
}

}
