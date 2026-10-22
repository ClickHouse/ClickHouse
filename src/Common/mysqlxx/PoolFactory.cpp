#include <map>

#include <mysqlxx/PoolFactory.h>
#include <Common/SipHash.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace mysqlxx
{

struct PoolFactory::Impl
{
    /// Cache of shared pools keyed by everything the pool constructors read from the configuration:
    /// the endpoint, the credentials, the per-connection settings and the pool settings.
    std::map<std::string, std::shared_ptr<PoolWithFailover>> pools;

    std::mutex mutex;
};

PoolWithFailover PoolFactory::get(const std::string & config_name, unsigned default_connections,
    unsigned max_connections, size_t max_tries)
{
    return get(Poco::Util::Application::instance().config(), config_name, default_connections, max_connections, max_tries);
}

std::string getPoolEntryName(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name, unsigned default_max_connections)
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

    /// Resolves a connection parameter with the same lookup order as `Pool::Pool`: a replica-level
    /// value first, the parent config as the fallback. For the parent config itself both lookups are
    /// the same key, so the lambda works for the non-replica form as well.
    auto get_param = [&](const std::string & prefix, const std::string & key)
    {
        const std::string replica_key = prefix + "." + key;
        return config.has(replica_key) ? config.getString(replica_key) : config.getString(config_name + "." + key, "");
    };

    /// Resolves a boolean connection parameter with the same lookup order as `Pool::Pool`.
    auto get_bool_param = [&](const std::string & prefix, const std::string & key, bool fallback)
    {
        const std::string replica_key = prefix + "." + key;
        return config.has(replica_key) ? config.getBool(replica_key) : config.getBool(config_name + "." + key, fallback);
    };

    /// The credentials belong to the connection, so two sources that share an endpoint but not their
    /// credentials must not share a pool: one of them would authenticate as the other. A hash keeps
    /// the password and the certificates themselves out of the key. Folded into the segment of the
    /// replica the credentials belong to.
    auto credentials_part = [&](const std::string & prefix)
    {
        SipHash hash;
        for (const auto & key : {"password", "ssl_ca", "ssl_cert", "ssl_key", "ssl_ca_pem", "ssl_cert_pem", "ssl_key_pem"})
        {
            /// Frame every field with its length: a raw concatenation would let distinct tuples
            /// collapse to the same byte stream (`("ab", "c")` vs `("a", "bc")`) and alias the pools.
            const auto value = get_param(prefix, key);
            hash.update(value.size());
            hash.update(value.data(), value.size());
        }
        return "&credentials=" + std::to_string(hash.get64());
    };

    /// `Pool::Pool` connects over a Unix socket when one is configured, so the socket path is a part
    /// of the endpoint: two sources with the same host, port, user and database but different sockets
    /// talk to different MySQL instances and must not share a pool.
    auto socket_part = [&](const std::string & prefix) { return "&socket=" + get_param(prefix, "socket"); };

    /// The rest of what `Pool::Pool` reads from the configuration. These do not change where the
    /// connection goes, but they change how it behaves, so a shared pool must not be reused for a
    /// source that asked for different values: whichever source constructed the pool first would
    /// otherwise silently dictate the timeouts and the reconnect semantics of all the others.
    /// `connect_timeout` and `rw_timeout` fall back to the process-wide `mysql_connect_timeout` /
    /// `mysql_rw_timeout` rather than to the parent configuration - mirror that exactly.
    auto connection_settings_part = [&](const std::string & prefix)
    {
        const int connect_timeout
            = config.getInt(prefix + ".connect_timeout", config.getInt("mysql_connect_timeout", MYSQLXX_DEFAULT_TIMEOUT));
        const int rw_timeout = config.getInt(prefix + ".rw_timeout", config.getInt("mysql_rw_timeout", MYSQLXX_DEFAULT_RW_TIMEOUT));
        const bool local_infile = get_bool_param(prefix, "enable_local_infile", MYSQLXX_DEFAULT_ENABLE_LOCAL_INFILE);
        const bool opt_reconnect = get_bool_param(prefix, "opt_reconnect", MYSQLXX_DEFAULT_MYSQL_OPT_RECONNECT);

        return "&connect_timeout=" + std::to_string(connect_timeout) + "&rw_timeout=" + std::to_string(rw_timeout)
            + "&local_infile=" + (local_infile ? "1" : "0") + "&opt_reconnect=" + (opt_reconnect ? "1" : "0");
    };

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
                std::string tmp_db = config.getString(replica_name + ".db", db);

                /// Resolve compression per replica: replica-level value takes priority,
                /// falling back to the parent config (same lookup order as Pool::Pool).
                std::string tmp_compression = config.getBool(replica_name + ".enable_compression", parent_compression) ? "1" : "0";

                /// The priority orders the replicas inside the pool, so it is a property of the pool too.
                const int priority = config.getInt(replica_name + ".priority", 0);

                entry_name += (entry_name.empty() ? "" : "|") + tmp_user + "@" + tmp_host + ":" + tmp_port + "/" + tmp_db
                    + "?compression=" + tmp_compression + "&priority=" + std::to_string(priority) + socket_part(replica_name)
                    + connection_settings_part(replica_name) + credentials_part(replica_name);
            }
        }
    }
    else
    {
        std::string compression_value = parent_compression ? "1" : "0";
        entry_name = user + "@" + host + ":" + port + "/" + db + "?compression=" + compression_value + socket_part(config_name)
            + connection_settings_part(config_name) + credentials_part(config_name);
    }

    /// `connection_pool_size` and `connection_wait_timeout` describe the shared pool itself (a single
    /// physical pool cannot have two different sizes or wait semantics). They are read at the parent
    /// config level by PoolWithFailover's config constructor, so include them in the cache key:
    /// dictionaries pointing at the same endpoint but requesting different pool settings must get
    /// separate pools instead of silently inheriting the settings of whichever dictionary created the
    /// cached pool first.
    /// `background_reconnect` is read at the same level and decides whether the replicas of this pool
    /// are registered in `ReplicasReconnector`, which is also a property of the pool as a whole.
    const unsigned pool_size = config.getUInt(config_name + ".connection_pool_size", default_max_connections);
    const auto wait_timeout = config.getUInt64(config_name + ".connection_wait_timeout", MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_CONNECTION_WAIT_TIMEOUT);
    const bool bg_reconnect = config.getBool(config_name + ".background_reconnect", false);
    entry_name += "&pool_size=" + std::to_string(pool_size) + "&wait_timeout=" + std::to_string(wait_timeout)
        + "&background_reconnect=" + (bg_reconnect ? "1" : "0");

    return entry_name;
}

PoolWithFailover PoolFactory::get(const Poco::Util::AbstractConfiguration & config,
        const std::string & config_name, unsigned default_connections, unsigned max_connections, size_t max_tries)
{
    std::lock_guard lock(impl->mutex);

    std::string entry_name = getPoolEntryName(config, config_name, max_connections);

    /// For shared pools (share_connection=true), entry_name encodes the actual connection
    /// parameters and settings. Use it as the cache key instead of
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
