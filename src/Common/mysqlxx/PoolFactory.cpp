#include <map>

#include <mysqlxx/PoolFactory.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace mysqlxx
{

struct PoolFactory::Impl
{
    /// Cache of shared pools keyed by connection parameters (host, port, user, db, compression).
    std::map<std::string, std::shared_ptr<PoolWithFailover>> pools;

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

    /// Parent-level compression setting; used as fallback for replicas that do not override it.
    bool parent_compression = config.getBool(config_name + ".enable_compression", false);

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

                /// Resolve compression per replica: replica-level value takes priority,
                /// falling back to the parent config (same lookup order as Pool::Pool).
                std::string tmp_compression = config.getBool(replica_name + ".enable_compression", parent_compression) ? "1" : "0";

                entry_name += (entry_name.empty() ? "" : "|") + tmp_user + "@" + tmp_host + ":" + tmp_port + "/" + db + "?compression=" + tmp_compression;
            }
        }
    }
    else
    {
        std::string compression_value = parent_compression ? "1" : "0";
        entry_name = user + "@" + host + ":" + port + "/" + db + "?compression=" + compression_value;
    }
    return entry_name;
}

PoolWithFailover PoolFactory::get(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name, unsigned default_connections, unsigned max_connections, size_t max_tries)
{
    std::lock_guard lock(impl->mutex);

    std::string entry_name = getPoolEntryName(config, config_name);

    /// For shared pools (share_connection=true), entry_name encodes the actual connection
    /// parameters (host, port, user, db, compression). Use it as the cache key instead of
    /// config_name, because per-dictionary XML configs all share the same config path prefix
    /// (e.g. "dictionary.source.mysql"), so keying by config_name alone would cause dicts
    /// with different enable_compression values to incorrectly share a single pool.
    const std::string & pool_key = entry_name.empty() ? config_name : entry_name;

    auto entry = impl->pools.find(pool_key);
    if (entry != impl->pools.end())
        return *(entry->second);

    auto pool = std::make_shared<PoolWithFailover>(config, config_name, default_connections, max_connections, max_tries);
    if (!entry_name.empty())
        impl->pools.insert_or_assign(pool_key, pool);
    return *pool;
}

void PoolFactory::reset()
{
    std::lock_guard lock(impl->mutex);
    impl->pools.clear();
}

PoolFactory::PoolFactory() : impl(std::make_unique<PoolFactory::Impl>()) {}

PoolFactory & PoolFactory::instance()
{
    static PoolFactory ret;
    return ret;
}

}
