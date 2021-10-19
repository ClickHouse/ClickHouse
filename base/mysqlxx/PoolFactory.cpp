#include <mysqlxx/PoolFactory.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace mysqlxx
{

struct PoolFactory::Impl
{
    // Cache of already affected pools identified by their config name
    std::map<std::string, std::shared_ptr<PoolWithFailover>> pools;

    // Cache of Pool ID (host + port + user +...) cibling already established shareable pool
    std::map<std::string, std::string> pools_by_ids;

    /// Protect pools and pools_by_ids caches
    std::mutex mutex;
};

PoolWithFailover PoolFactory::get(const std::string & config_name, unsigned default_connections,
    unsigned max_connections, size_t max_tries)
{
    return get(Poco::Util::Application::instance().config(), config_name, default_connections, max_connections, max_tries);
}

/// Duplicate of code from StringUtils.h. Copied here for less dependencies.
static bool startsWith(const std::string & s, const char * prefix)
{
    return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
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
    std::string table = config.getString(config_name + ".table", "");

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    if (config.has(config_name + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys replica_keys;
        config.keys(config_name, replica_keys);
        for (const auto & replica_config_key : replica_keys)
        {
            /// There could be another elements in the same level in configuration file, like "user", "port"...
            if (startsWith(replica_config_key, "replica"))
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

    std::lock_guard<std::mutex> lock(impl->mutex);
    if (auto entry = impl->pools.find(config_name); entry != impl->pools.end())
    {
        return *(entry->second.get());
    }
    else
    {
        std::string entry_name = getPoolEntryName(config, config_name);
        if (auto id = impl->pools_by_ids.find(entry_name); id != impl->pools_by_ids.end())
        {
            entry = impl->pools.find(id->second);
            std::shared_ptr<PoolWithFailover> pool = entry->second;
            impl->pools.insert_or_assign(config_name, pool);
            return *pool;
        }

        auto pool = std::make_shared<PoolWithFailover>(config, config_name, default_connections, max_connections, max_tries);
        // Check the pool will be shared
        if (!entry_name.empty())
        {
            // Store shared pool
            impl->pools.insert_or_assign(config_name, pool);
            impl->pools_by_ids.insert_or_assign(entry_name, config_name);
        }
        return *(pool.get());
    }
}

void PoolFactory::reset()
{
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pools.clear();
    impl->pools_by_ids.clear();
}

PoolFactory::PoolFactory() : impl(std::make_unique<PoolFactory::Impl>()) {}

PoolFactory & PoolFactory::instance()
{
    static PoolFactory ret;
    return ret;
}

}
