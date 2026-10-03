#pragma once

#include <base/types.h>

#include <map>
#include <optional>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB::Proxy
{

/// Protocols the proxy can serve on a listening port.
enum class ListenerProtocol : uint8_t
{
    HTTP,          /// ClickHouse HTTP interface. Also serves /ping, static content and the proxy status page.
    Native,        /// ClickHouse native TCP protocol.
    MySQL,         /// MySQL wire protocol.
    PostgreSQL,    /// PostgreSQL wire protocol.
    SSH,           /// SSH: terminated by the proxy to route by the offered public key, then re-originated.
    TLS,           /// Transparent TLS routing by SNI, without decryption.
    Stream,        /// Opaque TCP forwarding (gRPC, Arrow Flight, or any other TCP protocol).
};

std::string_view toString(ListenerProtocol protocol);
ListenerProtocol parseListenerProtocol(const String & name);

struct ListenerConfig
{
    ListenerProtocol protocol = ListenerProtocol::HTTP;
    String host;                        /// Empty means the global listen_host.
    UInt16 port = 0;
    bool secure = false;                /// Terminate TLS from the first byte (https, secure native protocol).
                                        /// MySQL and PostgreSQL switch to TLS in-band instead, and the TLS protocol never decrypts.
    String default_pool;                /// Used when no routing rule matches. May be empty if rules always match.
};

struct BackendConfig
{
    String name;                        /// Used for logging and consistent hashing. Defaults to the host and
                                        /// every configured port, so distinct endpoints never share a name.
    String host;

    /// Per-protocol ports of the backend server. Zero means the default port of the protocol
    /// (for a secure backend, the default secure port: 9440 for native, 8443 for HTTP).
    UInt16 tcp_port = 0;
    UInt16 http_port = 0;
    UInt16 mysql_port = 0;
    UInt16 postgresql_port = 0;
    UInt16 ssh_port = 0;
    UInt16 raw_port = 0;                /// For TLS and Stream listeners. Zero means the same port as the listener.
    UInt16 health_check_port = 0;       /// The port probed by the health monitor. Zero means the first configured
                                        /// port (native, HTTP, MySQL, PostgreSQL, SSH, raw), or the default native port.

    bool secure = false;                /// Encrypt the proxy-to-backend leg.
    UInt32 weight = 1;

    /// Credentials for polling the backend for resource usage over HTTP. Polling is enabled if the user is not empty.
    String monitor_user;
    String monitor_password;
};

struct StickinessConfig
{
    bool by_session_id = false;         /// Prefer the same backend for the same session_id from the HTTP URL.
    bool by_peer_address = false;       /// Prefer the same backend for connections from the same client address.
};

struct PoolConfig
{
    String name;
    String load_balancing = "least_connections";
    std::vector<BackendConfig> backends;
    std::optional<StickinessConfig> stickiness;   /// Overrides the global stickiness settings.
};

/// A routing rule. All specified matchers must match (logical AND).
/// Rules are checked in the order of definition; the first match wins.
struct RuleConfig
{
    String host;                        /// Exact value or comma-separated list of values.
    String host_regexp;                 /// RE2 regular expression; must match the whole value. May contain capture groups.
    String user;
    String user_regexp;
    String database;
    String database_regexp;
    String query_type;                  /// select, insert or other; comma-separated list.
    String protocol;                    /// Restrict the rule to listeners of these protocols; comma-separated list.
    String authorized_key;              /// SSH public key(s) the offered key must equal; each is "<type> <base64>". Newline/comma-separated.
    String authorized_key_file;         /// Path to an `authorized_keys` file whose keys this rule matches.

    String pool;                        /// The target pool.
    std::optional<BackendConfig> backend_template;  /// Alternatively, a backend address where $1..$9 are replaced
                                                    /// with regexp captures (numbered across host, user, database matchers).
};

struct HooksConfig
{
    /// Every hook is a shell command run as: command KIND PROTOCOL HOST USER DATABASE.
    /// Hooks are not run for SSH connections: SSH is routed inside a libssh authentication callback.
    String on_unknown;                  /// No rule matched and the listener has no default pool. A notification:
                                        /// the routing table is static, so the connection is rejected anyway.
    String on_no_backends;              /// All backends of the chosen pool are unavailable. After the command
                                        /// succeeds, the proxy waits for a backend of the pool to come up.
    String on_first_seen_user;          /// A user name is seen for the first time. Must be idempotent: it is
                                        /// re-run after a failure, and after the name is evicted from the
                                        /// bounded set of seen names.
    String on_first_seen_database;      /// A database name is seen for the first time. Same as above.
    UInt64 timeout_ms = 60000;          /// The limit on the run time of a hook (it is killed after that),
                                        /// and on the wait for backends to become available after a hook.
};

struct HealthCheckConfig
{
    bool enabled = true;
    UInt64 interval_ms = 5000;
    UInt64 timeout_ms = 3000;
    UInt32 failures_to_mark_down = 3;
    UInt64 resource_poll_interval_ms = 10000;
    String resource_query;              /// Query for resource polling. Must return one row in TSV: CPU usage in cores, memory usage in bytes.
};

struct SSHConfig
{
    /// The proxy's own host key, presented to clients when it terminates SSH. Required for `ssh` listeners.
    String host_key_file;
    String banner = "ClickHouse-proxy";

    /// Credentials for the proxy-to-backend leg (re-origination): a private key the backends trust.
    /// The client is authenticated by the proxy through its offered public key; the proxy then logs
    /// in to the backend as a bastion.
    String backend_user = "default";
    String backend_key_file;

    /// An OpenSSH `known_hosts` file with the host keys of the backends. Required for `ssh` listeners:
    /// the proxy logs in to a backend with its bastion key only after verifying the backend's host key.
    String known_hosts_file;

    UInt64 auth_timeout_ms = 10000;
};

struct StaticPageConfig
{
    String path;                        /// Exact URL path.
    String file;                        /// Path to a file with the response body (re-read when modified), or
    String content;                     /// inline response body.
    String content_type = "text/html; charset=UTF-8";
};

struct HTTPConfig
{
    String ping_path = "/ping";
    String status_path; /// JSON with backends health and statistics. Empty string disables it.
    bool add_x_forwarded_for = false;
    std::vector<StaticPageConfig> static_pages;
};

struct ProxyConfiguration
{
    String listen_host = "0.0.0.0";
    UInt32 listen_backlog = 4096;

    std::vector<ListenerConfig> listeners;
    std::map<String, PoolConfig> pools;
    std::vector<RuleConfig> rules;
    StickinessConfig stickiness;
    HooksConfig hooks;
    HealthCheckConfig health_check;
    HTTPConfig http;
    SSHConfig ssh;

    UInt64 connect_timeout_ms = 3000;
    UInt64 handshake_timeout_ms = 10000;
    UInt64 send_timeout_ms = 300000;
    /// Per-direction relay buffer (for a splice relay, the pipe/chunk size). Larger values raise bulk
    /// throughput: 16 KiB caps a single stream well under 1 GB/s, 64 KiB reaches ~2 GB/s, and 256 KiB
    /// (the default) reaches near line rate. The cost is ~2x this much memory per actively-transferring
    /// connection, so lower it for very many mostly-idle connections.
    size_t relay_buffer_size = 262144;
    UInt32 fiber_stack_size = 512 * 1024;

    /// The limit on the number of backends created from `backend_template` rules. The template captures
    /// come from the client, so the set is bounded: idle dynamic backends are evicted when it is full,
    /// and a connection that would need a new one beyond the limit is rejected.
    size_t max_dynamic_backends = 10000;

    static ProxyConfiguration load(const Poco::Util::AbstractConfiguration & config);
};

/// The backend port to connect to for a given listener protocol.
UInt16 backendPortFor(ListenerProtocol protocol, const BackendConfig & backend, UInt16 listener_port);

/// The backend port probed by the health monitor.
UInt16 healthCheckPort(const BackendConfig & backend);

}
