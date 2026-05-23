#include <mysqlxx/PoolFactory.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace mysqlxx
{

struct PoolFactory::Impl
{
    // Cache of Pool ID (host + port + user +...) cibling already established shareable pool
    std::map<std::string, std::shared_ptr<PoolWithFailover>> shared_pools_by_id;

    /// Protect pools and pools_by_ids caches
    std::mutex mutex;
};

PoolWithFailover PoolFactory::get(const std::string & config_name, unsigned default_connections,
    unsigned max_connections, size_t max_tries)
{
    return get(Poco::Util::Application::instance().config(), config_name, default_connections, max_connections, max_tries);
}

static std::string getPoolEntryName(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name)
{
    bool shared = config.getBool(config_name + ".share_connection", false);

    // Not shared no need to generate a name the pool won't be stored
    if (!shared)
        return "";

    std::string entry_name;
    std::string host = config.getString(config_name + ".host", "");
    std::string port = config.getString(config_name + ".port", "");
    std::string user = config.getString(config_name + ".user", "");
    std::string db = config.getString(config_name + ".db", "");

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    if (config.has(config_name + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys replica_keys;
        config.keys(config_name, replica_keys);
        for (const auto & replica_config_key : replica_keys)
        {
            /// There could be another elements in the same level in configuration file, like "user", "port"...
            if (replica_config_key.starts_with("replica"))
            {
                std::string replica_name = config_name + "." + replica_config_key;
                std::string tmp_host = config.getString(replica_name + ".host", host);
                std::string tmp_port = config.getString(replica_name + ".port", port);
                std::string tmp_user = config.getString(replica_name + ".user", user);
                entry_name += (entry_name.empty() ? "" : "|") + tmp_user + "@" + tmp_host + ":" + tmp_port + "/" + db;
            }
        }
    }
    else
    {
        entry_name = user + "@" + host + ":" + port + "/" + db;
    }
    return entry_name;
}

PoolWithFailover PoolFactory::get(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name, unsigned default_connections, unsigned max_connections, size_t max_tries)
{
    std::lock_guard lock(impl->mutex);

    const std::string entry_name = getPoolEntryName(config, config_name);

    if (entry_name.empty())
        return PoolWithFailover(config, config_name, default_connections, max_connections, max_tries);

    if (auto it = impl->shared_pools_by_id.find(entry_name); it != impl->shared_pools_by_id.end())
        return *(it->second);

    auto pool = std::make_shared<PoolWithFailover>(config, config_name, default_connections, max_connections, max_tries);
    impl->shared_pools_by_id.emplace(entry_name, pool);

    return *pool;
}

void PoolFactory::reset()
{
    std::lock_guard lock(impl->mutex);
    impl->shared_pools_by_id.clear();
}

PoolFactory::PoolFactory() : impl(std::make_unique<PoolFactory::Impl>()) {}

PoolFactory & PoolFactory::instance()
{
    static PoolFactory ret;
    return ret;
}

}
