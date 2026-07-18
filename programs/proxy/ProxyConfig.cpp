#include <ProxyConfig.h>

#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <limits>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NO_ELEMENTS_IN_CONFIG;
}
}

namespace DB::Proxy
{

std::string_view toString(ListenerProtocol protocol)
{
    switch (protocol)
    {
        case ListenerProtocol::HTTP: return "http";
        case ListenerProtocol::Native: return "native";
        case ListenerProtocol::MySQL: return "mysql";
        case ListenerProtocol::PostgreSQL: return "postgresql";
        case ListenerProtocol::SSH: return "ssh";
        case ListenerProtocol::TLS: return "tls";
        case ListenerProtocol::Stream: return "stream";
    }
}

ListenerProtocol parseListenerProtocol(const String & name)
{
    if (name == "http")
        return ListenerProtocol::HTTP;
    if (name == "native" || name == "tcp")
        return ListenerProtocol::Native;
    if (name == "mysql")
        return ListenerProtocol::MySQL;
    if (name == "postgresql" || name == "postgres")
        return ListenerProtocol::PostgreSQL;
    if (name == "ssh")
        return ListenerProtocol::SSH;
    if (name == "tls")
        return ListenerProtocol::TLS;
    if (name == "stream")
        return ListenerProtocol::Stream;
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown listener protocol '{}'. Supported protocols: http, native, mysql, postgresql, ssh, tls, stream", name);
}

static PeekMode parsePeekMode(const String & name)
{
    if (name == "auto")
        return PeekMode::Auto;
    if (name == "none")
        return PeekMode::None;
    if (name == "credentials")
        return PeekMode::Credentials;
    if (name == "query")
        return PeekMode::Query;
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown peek mode '{}'. Supported modes: auto, none, credentials, query", name);
}

UInt16 backendPortFor(ListenerProtocol protocol, const BackendConfig & backend, UInt16 listener_port)
{
    switch (protocol)
    {
        case ListenerProtocol::HTTP:
            return backend.http_port ? backend.http_port : 8123;
        case ListenerProtocol::Native:
            return backend.tcp_port ? backend.tcp_port : 9000;
        case ListenerProtocol::MySQL:
            return backend.mysql_port ? backend.mysql_port : 9004;
        case ListenerProtocol::PostgreSQL:
            return backend.postgresql_port ? backend.postgresql_port : 9005;
        case ListenerProtocol::SSH:
            return backend.ssh_port ? backend.ssh_port : 9022;
        case ListenerProtocol::TLS:
        case ListenerProtocol::Stream:
            return backend.raw_port ? backend.raw_port : listener_port;
    }
}

/// Reads a required listening port and validates it fits in [1, 65535].
/// Without this check `static_cast<UInt16>` would silently wrap out-of-range
/// values (e.g. 70000 -> 4464), binding or connecting to the wrong port.
static UInt16 parseRequiredPort(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    const UInt64 value = config.getUInt64(key);
    if (value == 0 || value > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Invalid port {} in '{}': a listening port must be between 1 and {}", value, key, std::numeric_limits<UInt16>::max());
    return static_cast<UInt16>(value);
}

/// Reads an optional backend port (0 means "not set, use the protocol default")
/// and validates it does not overflow UInt16.
static UInt16 parseOptionalPort(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    const UInt64 value = config.getUInt64(key, 0);
    if (value > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Invalid port {} in '{}': a port must not exceed {}", value, key, std::numeric_limits<UInt16>::max());
    return static_cast<UInt16>(value);
}

static BackendConfig loadBackend(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    BackendConfig backend;
    backend.host = config.getString(prefix + ".host");
    backend.tcp_port = parseOptionalPort(config, prefix + ".tcp_port");
    backend.http_port = parseOptionalPort(config, prefix + ".http_port");
    backend.mysql_port = parseOptionalPort(config, prefix + ".mysql_port");
    backend.postgresql_port = parseOptionalPort(config, prefix + ".postgresql_port");
    backend.ssh_port = parseOptionalPort(config, prefix + ".ssh_port");
    backend.raw_port = parseOptionalPort(config, prefix + ".raw_port");
    backend.secure = config.getBool(prefix + ".secure", false);
    backend.weight = config.getUInt(prefix + ".weight", 1);
    backend.monitor_user = config.getString(prefix + ".monitor_user", "");
    backend.monitor_password = config.getString(prefix + ".monitor_password", "");
    backend.name = config.getString(prefix + ".name", backend.host + ":" + std::to_string(backend.tcp_port ? backend.tcp_port : 9000));

    if (backend.weight == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Backend '{}' has zero weight", backend.name);

    return backend;
}

ProxyConfiguration ProxyConfiguration::load(const Poco::Util::AbstractConfiguration & config)
{
    ProxyConfiguration res;

    if (!config.has("proxy"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "The configuration file has no 'proxy' section");

    res.listen_host = config.getString("proxy.listen_host", "0.0.0.0");
    res.listen_backlog = config.getUInt("proxy.listen_backlog", 4096);
    res.display_name = config.getString("proxy.display_name", "ClickHouse proxy");
    res.advertised_tcp_protocol_version = config.getUInt64("proxy.advertised_tcp_protocol_version", 0);
    res.connect_timeout_ms = config.getUInt64("proxy.connect_timeout_ms", 3000);
    res.handshake_timeout_ms = config.getUInt64("proxy.handshake_timeout_ms", 10000);
    res.send_timeout_ms = config.getUInt64("proxy.send_timeout_ms", 300000);
    res.relay_buffer_size = config.getUInt64("proxy.relay_buffer_size", 262144);
    res.fiber_stack_size = config.getUInt("proxy.fiber_stack_size", 512 * 1024);

    Poco::Util::AbstractConfiguration::Keys keys;

    /// Pools.

    config.keys("proxy.pools", keys);
    for (const auto & key : keys)
    {
        if (key != "pool" && !key.starts_with("pool["))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unexpected element '{}' in 'proxy.pools'", key);

        const String prefix = "proxy.pools." + key;

        PoolConfig pool;
        pool.name = config.getString(prefix + ".name");
        pool.load_balancing = config.getString(prefix + ".load_balancing", "least_connections");

        if (config.has(prefix + ".stickiness"))
        {
            StickinessConfig stickiness;
            stickiness.by_session_id = config.getBool(prefix + ".stickiness.by_session_id", false);
            stickiness.by_peer_address = config.getBool(prefix + ".stickiness.by_peer_address", false);
            pool.stickiness = stickiness;
        }

        Poco::Util::AbstractConfiguration::Keys pool_keys;
        config.keys(prefix, pool_keys);
        for (const auto & pool_key : pool_keys)
        {
            if (pool_key == "backend" || pool_key.starts_with("backend["))
                pool.backends.push_back(loadBackend(config, prefix + "." + pool_key));
        }

        if (pool.backends.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Pool '{}' has no backends", pool.name);

        if (res.pools.contains(pool.name))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Duplicate pool name '{}'", pool.name);

        std::unordered_set<String> backend_names;
        for (const auto & backend : pool.backends)
            if (!backend_names.insert(backend.name).second)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Duplicate backend name '{}' in pool '{}'. Specify distinct 'name' elements", backend.name, pool.name);

        res.pools.emplace(pool.name, std::move(pool));
    }

    /// Listeners.

    config.keys("proxy.listeners", keys);
    for (const auto & key : keys)
    {
        if (key != "listener" && !key.starts_with("listener["))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unexpected element '{}' in 'proxy.listeners'", key);

        const String prefix = "proxy.listeners." + key;

        ListenerConfig listener;
        listener.protocol = parseListenerProtocol(config.getString(prefix + ".protocol"));
        listener.host = config.getString(prefix + ".host", "");
        listener.port = parseRequiredPort(config, prefix + ".port");
        listener.secure = config.getBool(prefix + ".secure", false);
        listener.peek = parsePeekMode(config.getString(prefix + ".peek", "auto"));
        listener.default_pool = config.getString(prefix + ".pool", "");

        if (listener.secure && (listener.protocol == ListenerProtocol::MySQL || listener.protocol == ListenerProtocol::PostgreSQL))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Listener on port {}: MySQL and PostgreSQL switch to TLS in-band, remove <secure>", listener.port);

        if (listener.secure && listener.protocol == ListenerProtocol::TLS)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Listener on port {}: the 'tls' protocol forwards TLS transparently and never terminates it, remove <secure>", listener.port);

        if (listener.secure && listener.protocol == ListenerProtocol::SSH)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Listener on port {}: SSH is not layered over TLS, remove <secure>", listener.port);

        if (!listener.default_pool.empty() && !res.pools.contains(listener.default_pool))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Listener on port {} references unknown pool '{}'", listener.port, listener.default_pool);

        res.listeners.push_back(std::move(listener));
    }

    if (res.listeners.empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "The 'proxy' section has no listeners");

    /// Routing rules.

    if (config.has("proxy.rules"))
    {
        config.keys("proxy.rules", keys);
        for (const auto & key : keys)
        {
            if (key != "rule" && !key.starts_with("rule["))
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unexpected element '{}' in 'proxy.rules'", key);

            const String prefix = "proxy.rules." + key;

            RuleConfig rule;
            rule.host = config.getString(prefix + ".host", "");
            rule.host_regexp = config.getString(prefix + ".host_regexp", "");
            rule.user = config.getString(prefix + ".user", "");
            rule.user_regexp = config.getString(prefix + ".user_regexp", "");
            rule.database = config.getString(prefix + ".database", "");
            rule.database_regexp = config.getString(prefix + ".database_regexp", "");
            rule.query_type = config.getString(prefix + ".query_type", "");
            rule.protocol = config.getString(prefix + ".protocol", "");
            rule.authorized_key = config.getString(prefix + ".authorized_key", "");
            rule.authorized_key_file = config.getString(prefix + ".authorized_key_file", "");
            rule.pool = config.getString(prefix + ".pool", "");

            if (config.has(prefix + ".backend_template"))
                rule.backend_template = loadBackend(config, prefix + ".backend_template");

            if (rule.pool.empty() == !rule.backend_template)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "A routing rule must have exactly one of 'pool' and 'backend_template'");

            if (!rule.pool.empty() && !res.pools.contains(rule.pool))
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "A routing rule references unknown pool '{}'", rule.pool);

            if (!rule.host.empty() && !rule.host_regexp.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "A routing rule has both 'host' and 'host_regexp'");
            if (!rule.user.empty() && !rule.user_regexp.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "A routing rule has both 'user' and 'user_regexp'");
            if (!rule.database.empty() && !rule.database_regexp.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "A routing rule has both 'database' and 'database_regexp'");

            res.rules.push_back(std::move(rule));
        }
    }

    /// The rest.

    res.stickiness.by_session_id = config.getBool("proxy.stickiness.by_session_id", false);
    res.stickiness.by_peer_address = config.getBool("proxy.stickiness.by_peer_address", false);

    res.hooks.on_unknown = config.getString("proxy.hooks.on_unknown", "");
    res.hooks.on_no_backends = config.getString("proxy.hooks.on_no_backends", "");
    res.hooks.on_first_seen_user = config.getString("proxy.hooks.on_first_seen_user", "");
    res.hooks.on_first_seen_database = config.getString("proxy.hooks.on_first_seen_database", "");
    res.hooks.timeout_ms = config.getUInt64("proxy.hooks.timeout_ms", 60000);

    res.health_check.enabled = config.getBool("proxy.health_check.enabled", true);
    res.health_check.interval_ms = config.getUInt64("proxy.health_check.interval_ms", 5000);
    res.health_check.timeout_ms = config.getUInt64("proxy.health_check.timeout_ms", 3000);
    res.health_check.failures_to_mark_down = config.getUInt("proxy.health_check.failures_to_mark_down", 3);
    res.health_check.resource_poll_interval_ms = config.getUInt64("proxy.health_check.resource_poll_interval_ms", 10000);
    res.health_check.resource_query = config.getString("proxy.health_check.resource_query",
        "SELECT (SELECT sum(value) FROM system.asynchronous_metrics WHERE metric IN ('OSUserTimeNormalized', 'OSSystemTimeNormalized')),"
        " (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryResident') FORMAT TSV");

    res.ssh.host_key_file = config.getString("proxy.ssh.host_key_file", "");
    res.ssh.banner = config.getString("proxy.ssh.banner", "ClickHouse-proxy");
    res.ssh.backend_user = config.getString("proxy.ssh.backend_user", "default");
    res.ssh.backend_key_file = config.getString("proxy.ssh.backend_key_file", "");
    res.ssh.auth_timeout_ms = config.getUInt64("proxy.ssh.auth_timeout_ms", 10000);

    for (const auto & listener : res.listeners)
    {
        if (listener.protocol == ListenerProtocol::SSH && res.ssh.host_key_file.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "An 'ssh' listener requires the proxy host key in <proxy><ssh><host_key_file>");
    }

    res.http.ping_path = config.getString("proxy.http.ping_path", "/ping");
    res.http.status_path = config.getString("proxy.http.status_path", "/proxy_status");
    res.http.add_x_forwarded_for = config.getBool("proxy.http.add_x_forwarded_for", false);

    if (config.has("proxy.http.static"))
    {
        config.keys("proxy.http.static", keys);
        for (const auto & key : keys)
        {
            if (key != "page" && !key.starts_with("page["))
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unexpected element '{}' in 'proxy.http.static'", key);

            const String prefix = "proxy.http.static." + key;

            StaticPageConfig page;
            page.path = config.getString(prefix + ".path");
            page.file = config.getString(prefix + ".file", "");
            page.content = config.getString(prefix + ".content", "");
            page.content_type = config.getString(prefix + ".content_type", "text/html; charset=UTF-8");

            if (page.file.empty() == page.content.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Static page '{}' must have exactly one of 'file' and 'content'", page.path);

            res.http.static_pages.push_back(std::move(page));
        }
    }

    return res;
}

}
