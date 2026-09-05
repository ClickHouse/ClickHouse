#pragma once

#include "config.h"

#if USE_SILK

#include <ProxyConfig.h>
#include <Router.h>
#include <SocketIO.h>

#include <Common/Logger.h>

namespace DB::Proxy
{

/// Everything a per-connection handler needs. Shared (read-only) across all connections of a listener.
struct FrontendContext
{
    const ProxyConfiguration & config;
    const ListenerConfig & listener;
    Router & router;
#if USE_SSL
    Poco::Net::Context::Ptr server_tls_context;  /// For terminating TLS on secure listeners.
    Poco::Net::Context::Ptr client_tls_context;  /// For the proxy-to-backend TLS leg.
#endif
    LoggerPtr log;
};

/// Connect to a backend for the given listener protocol. When @p encrypt is set, the proxy-to-backend
/// leg is wrapped in TLS, using the backend's own host name as the server name and for certificate
/// verification; otherwise bytes are forwarded as-is (used for transparent TLS routing, where the
/// encrypted stream is passed through untouched).
/// Records connect latency and marks the backend down on repeated failures. Throws on failure.
FiberSocket connectToBackend(
    const FrontendContext & ctx,
    Backend & backend,
    bool encrypt);

/// Route the connection and, on success, connect to a backend. Returns a connected socket and sets
/// @p out_backend, or throws / returns an un-connected socket with @p out_backend left null.
struct RouteResult
{
    BackendPoolPtr pool;
    BackendPtr backend;
    String failure_reason;   /// Non-empty when no backend could be used.
};

RouteResult routeConnection(const FrontendContext & ctx, const RouteAttributes & attributes);

/// Route without inspecting the stream (by peer address or the listener's default pool) and splice the
/// connection to the chosen backend. Used for opaque streams and for MySQL (server-speaks-first, so the
/// proxy has nothing from the client to parse before choosing a backend).
void handleImmediatePassthrough(FiberSocket & client, const FrontendContext & ctx, RouteAttributes attributes);

/// Per-protocol connection handlers. The client socket is already TLS-terminated for secure listeners.
void handleHTTP(FiberSocket & client, const FrontendContext & ctx);
void handleNative(FiberSocket & client, const FrontendContext & ctx);
void handleMySQL(FiberSocket & client, const FrontendContext & ctx);
void handlePostgreSQL(FiberSocket & client, const FrontendContext & ctx);
void handlePassthrough(FiberSocket & client, const FrontendContext & ctx);

/// SSH is terminated by the proxy: it reads the client's offered public key, routes by it, and
/// re-originates a new SSH connection to the chosen backend (bastion). Takes ownership of @p fd.
void handleSSH(int fd, const FrontendContext & ctx);

/// Loads the configured SSH key files once, so that an unreadable or unparseable credential fails
/// server startup instead of every accepted connection. Called only when an 'ssh' listener exists.
void validateSSHKeys(const ProxyConfiguration & config);

/// Classify a SQL query as "select", "insert" or "other" by its leading keyword.
String classifyQuery(std::string_view query);

}

#endif
