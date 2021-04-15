#include <mysqlxx/PoolFactory.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace
{
    /// Duplicate of code from StringUtils.h. Copied here for less dependencies.
    bool startsWith(const std::string & s, const char * prefix)
    {
        return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
    }

    mysqlxx::ConnectionConfiguration parseConnectionConfiguration(const Poco::Util::AbstractConfiguration & cfg, const std::string & config_name, const char * parent_config_name_)
    {
        mysqlxx::ConnectionConfiguration configuration;

        if (parent_config_name_)
        {
            const std::string parent_config_name(parent_config_name_);
            configuration.db = cfg.getString(config_name + ".db", cfg.getString(parent_config_name + ".db", ""));
            configuration.user = cfg.has(config_name + ".user")
                ? cfg.getString(config_name + ".user")
                : cfg.getString(parent_config_name + ".user");
            configuration.password = cfg.has(config_name + ".password")
                ? cfg.getString(config_name + ".password")
                : cfg.getString(parent_config_name + ".password");

            if (!cfg.has(config_name + ".port") && !cfg.has(config_name + ".socket")
                && !cfg.has(parent_config_name + ".port") && !cfg.has(parent_config_name + ".socket"))
                throw Poco::Exception("mysqlxx::parse connectionConfiguration from abstract configuration: expected port or socket");

            configuration.port = cfg.has(config_name + ".port")
                ? cfg.getInt(config_name + ".port")
                : cfg.getInt(parent_config_name + ".port", 0);
            configuration.socket = cfg.has(config_name + ".socket")
                ? cfg.getString(config_name + ".socket")
                : cfg.getString(parent_config_name + ".socket", "");
            configuration.ssl_ca = cfg.has(config_name + ".ssl_ca")
                ? cfg.getString(config_name + ".ssl_ca")
                : cfg.getString(parent_config_name + ".ssl_ca", "");
            configuration.ssl_cert = cfg.has(config_name + ".ssl_cert")
                ? cfg.getString(config_name + ".ssl_cert")
                : cfg.getString(parent_config_name + ".ssl_cert", "");
            configuration.ssl_key = cfg.has(config_name + ".ssl_key")
                ? cfg.getString(config_name + ".ssl_key")
                : cfg.getString(parent_config_name + ".ssl_key", "");

            configuration.enable_local_infile = cfg.getBool(config_name + ".enable_local_infile",
                cfg.getBool(parent_config_name + ".enable_local_infile", mysqlxx::DEFAULT_ENABLE_LOCAL_INFILE));

            configuration.opt_reconnect = cfg.getBool(config_name + ".opt_reconnect",
                cfg.getBool(parent_config_name + ".opt_reconnect", mysqlxx::DEFAULT_MYSQL_OPT_RECONNECT));

        }
        else
        {
            configuration.db = cfg.getString(config_name + ".db", "");
            configuration.user = cfg.getString(config_name + ".user");
            configuration.password = cfg.getString(config_name + ".password");

            if (!cfg.has(config_name + ".port") && !cfg.has(config_name + ".socket"))
                throw Poco::Exception("mysqlxx::Pool configuration: expected port or socket");

            configuration.port = cfg.getInt(config_name + ".port", 0);
            configuration.socket = cfg.getString(config_name + ".socket", "");
            configuration.ssl_ca = cfg.getString(config_name + ".ssl_ca", "");
            configuration.ssl_cert = cfg.getString(config_name + ".ssl_cert", "");
            configuration.ssl_key = cfg.getString(config_name + ".ssl_key", "");

            configuration.enable_local_infile = cfg.getBool(
                config_name + ".enable_local_infile", mysqlxx::DEFAULT_ENABLE_LOCAL_INFILE);

            configuration.opt_reconnect = cfg.getBool(config_name + ".opt_reconnect", mysqlxx::DEFAULT_MYSQL_OPT_RECONNECT);
        }

        configuration.connect_timeout = cfg.getInt(config_name + ".connect_timeout",
            cfg.getInt("mysql_connect_timeout",
                mysqlxx::DEFAULT_TIMEOUT));

        configuration.rw_timeout =
            cfg.getInt(config_name + ".rw_timeout",
                cfg.getInt("mysql_rw_timeout",
                    mysqlxx::DEFAULT_RW_TIMEOUT));

        return configuration;
    }
}

namespace mysqlxx
{

struct PoolFactory::Impl
{
};

PoolFactory::PoolFactory() : impl(std::make_unique<PoolFactory::Impl>()) {}

PoolFactory & PoolFactory::instance()
{
    static PoolFactory ret;
    return ret;
}

std::shared_ptr<IPool> PoolFactory::getPoolWithFailover(const Poco::Util::AbstractConfiguration & config, const std::string & config_name) /// NOLINT
{
    bool share_connection = config.getBool(config_name + ".share_connection", false);
    bool close_connection = config.getBool(config_name + ".close_connection", false);

    std::vector<ReplicaConfiguration> replicas_configurations;

    if (config.has(config_name + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys replica_keys;
        config.keys(config_name, replica_keys);

        for (const auto & replica_config_key : replica_keys)
        {
            /// There could be another elements in the same level in configuration file, like "password", "port"...
            if (startsWith(replica_config_key, "replica"))
            {
                std::string replica_name = config_name + "." + replica_config_key;

                size_t priority = config.getUInt(replica_name + ".priority", 0);
                bool share_connection_replica = config.getBool(replica_name + ".share_connection", share_connection);
                bool close_connection_replica = config.getBool(replica_name + ".close_connection", close_connection);

                auto connection_configuration = parseConnectionConfiguration(config, replica_name, config_name.c_str());
                PoolConfiguration pool_configuration
                {
                    .share_connection = share_connection_replica,
                    .close_connection = close_connection_replica
                };

                ReplicaConfiguration replica_configuration
                {
                    .priority = priority,
                    .connection_configuration = std::move(connection_configuration),
                    .pool_configuration = std::move(pool_configuration)
                };

                replicas_configurations.emplace_back(replica_configuration);
            }
        }
    }
    else
    {
        auto connection_configuration = parseConnectionConfiguration(config, config_name, nullptr);

        PoolConfiguration pool_configuration
        {
            .share_connection = share_connection,
            .close_connection = close_connection
        };

        ReplicaConfiguration replica_configuration
        {
            .priority = 0,
            .connection_configuration = std::move(connection_configuration),
            .pool_configuration = std::move(pool_configuration)
        };

        replicas_configurations.emplace_back(replica_configuration);
    }

    return PoolWithFailover::create(replicas_configurations);
}

std::shared_ptr<IPool> PoolFactory::getPoolWithFailover(
    const std::string & database,
    const RemoteDescriptions & addresses,
    const std::string & user,
    const std::string & password)
{
    std::vector<ReplicaConfiguration> replicas_configurations;
    replicas_configurations.reserve(addresses.size());

    for (const auto & [host, port] : addresses)
    {
        ConnectionConfiguration connection_configuration;

        connection_configuration.db = database;
        connection_configuration.server = host;
        connection_configuration.port = port;
        connection_configuration.user = user;
        connection_configuration.password = password;

        ReplicaConfiguration replica_configuration
        {
            .priority = 0,
            .connection_configuration = connection_configuration,
            .pool_configuration = PoolConfiguration()
        };

        replicas_configurations.emplace_back(replica_configuration);
    }

    return mysqlxx::PoolWithFailover::create(replicas_configurations);
}

std::shared_ptr<IPool> PoolFactory::getPoolWithFailover(const ReplicasConfigurations & replicas_configurations)
{
    return mysqlxx::PoolWithFailover::create(replicas_configurations);
}

std::shared_ptr<IPool> PoolFactory::getPool(const Poco::Util::AbstractConfiguration & configuration, const std::string & config_name)
{
    ConnectionConfiguration connection_configuration = parseConnectionConfiguration(configuration, config_name, nullptr);
    return mysqlxx::Pool::create(connection_configuration);
}

std::shared_ptr<IPool> PoolFactory::getPool(const ConnectionConfiguration & connection_configuration, const PoolConfiguration & pool_configuration)
{
    return Pool::create(connection_configuration, pool_configuration);
}

void PoolFactory::reset()
{
    Pool::resetShareConnectionPools();
}

}

// /// Duplicate of code from StringUtils.h. Copied here for less dependencies.
// static bool startsWith(const std::string & s, const char * prefix)
// {
//     return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
// }

// static std::string getPoolEntryName(const Poco::Util::AbstractConfiguration & config,
//         const std::string & config_name)
// {
//     bool shared = config.getBool(config_name + ".share_connection", false);

//     // Not shared no need to generate a name the pool won't be stored
//     if (!shared)
//         return "";

//     std::string entry_name;
//     std::string host = config.getString(config_name + ".host", "");
//     std::string port = config.getString(config_name + ".port", "");
//     std::string user = config.getString(config_name + ".user", "");
//     std::string db = config.getString(config_name + ".db", "");
//     std::string table = config.getString(config_name + ".table", "");

//     Poco::Util::AbstractConfiguration::Keys keys;
//     config.keys(config_name, keys);

//     if (config.has(config_name + ".replica"))
//     {
//         Poco::Util::AbstractConfiguration::Keys replica_keys;
//         config.keys(config_name, replica_keys);
//         for (const auto & replica_config_key : replica_keys)
//         {
//             /// There could be another elements in the same level in configuration file, like "user", "port"...
//             if (startsWith(replica_config_key, "replica"))
//             {
//                 std::string replica_name = config_name + "." + replica_config_key;
//                 std::string tmp_host = config.getString(replica_name + ".host", host);
//                 std::string tmp_port = config.getString(replica_name + ".port", port);
//                 std::string tmp_user = config.getString(replica_name + ".user", user);
//                 entry_name += (entry_name.empty() ? "" : "|") + tmp_user + "@" + tmp_host + ":" + tmp_port + "/" + db;
//             }
//         }
//     }
//     else
//     {
//         entry_name = user + "@" + host + ":" + port + "/" + db;
//     }
//     return entry_name;
// }

// PoolWithFailover PoolFactory::get(const Poco::Util::AbstractConfiguration & config,
//         const std::string & config_name, unsigned, unsigned, size_t)
// {

//     std::lock_guard<std::mutex> lock(impl->mutex);
//     if (auto entry = impl->pools.find(config_name); entry != impl->pools.end())
//     {
//         return *(entry->second.get());
//     }
//     else
//     {
//         std::string entry_name = getPoolEntryName(config, config_name);
//         if (auto id = impl->pools_by_ids.find(entry_name); id != impl->pools_by_ids.end())
//         {
//             entry = impl->pools.find(id->second);
//             std::shared_ptr<PoolWithFailover> pool = entry->second;
//             impl->pools.insert_or_assign(config_name, pool);
//             return *pool;
//         }

//         std::terminate();

//         // auto pool = std::make_shared<PoolWithFailover>(config, config_name, default_connections, max_connections, max_tries);
//         // Check the pool will be shared
//         // if (!entry_name.empty())
//         // {
//         //     // Store shared pool
//         //     impl->pools.insert_or_assign(config_name, pool);
//         //     impl->pools_by_ids.insert_or_assign(entry_name, config_name);
//         // }
//         // return *(pool.get());
//     }
// }

// void PoolFactory::reset()
// {
//     std::lock_guard<std::mutex> lock(impl->mutex);
//     impl->pools.clear();
//     impl->pools_by_ids.clear();
// }

// PoolFactory::PoolFactory() : impl(std::make_unique<PoolFactory::Impl>()) {}

// PoolFactory & PoolFactory::instance()
// {
//     static PoolFactory ret;
//     return ret;
// }

// }
