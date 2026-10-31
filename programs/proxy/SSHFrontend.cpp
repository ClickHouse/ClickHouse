#include <Frontend.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <silk/fibers/fiber.h>

#include <base/scope_guard.h>

#include <Poco/Net/SocketAddress.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <optional>
#include <string>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

#if USE_SSH && defined(OS_LINUX)

#include <Access/SSH/SSHPublicKey.h>
#include <Server/SSH/SSHBind.h>
#include <Server/SSH/SSHSession.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#include <libssh/callbacks.h>
#include <libssh/libssh.h>
#include <libssh/server.h>
#pragma clang diagnostic pop

#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace DB::Proxy
{

#if USE_SSH && defined(OS_LINUX)

namespace
{

/// State shared between the libssh callbacks for one proxied SSH connection. The proxy terminates the
/// client's SSH (server leg), routes by the offered public key, and re-originates a new SSH connection
/// to the chosen backend (client leg), then splices the two channels. One Bridge lives on the stack of
/// the handler for the whole connection, so the callback structs it owns stay valid and are private to
/// this connection (concurrent connections each have their own).
/// Output of one direction of the splice that its destination channel cannot take yet, as its window is closed.
/// `ssh_channel_write` would wait for the window up to the session timeout and then leave the rest in the source
/// channel, where libssh offers it to the data callback again only when more data arrives on it. So the data
/// callbacks write only what fits in the window and keep the rest here, and the splice loop writes it as the
/// window reopens. The amount kept is bounded: beyond it, the callbacks leave the data in the source channel,
/// whose window then stays closed for the sender, and the splice loop reads it from there later.
struct PendingOutput
{
    std::deque<std::pair<bool, String>> segments;   /// (is_stderr, data), in order.
    size_t bytes = 0;
    bool eof_received = false;      /// The source sent EOF: forwarded once everything before it is written.
    bool eof_sent = false;
};

constexpr size_t max_pending_output_bytes = 1024 * 1024;

struct Bridge
{
    const FrontendContext * ctx = nullptr;
    Poco::Net::SocketAddress peer;
    LoggerPtr log;

    /// The pool is retained along with the backend: until the channel opens, the backend has no active
    /// connections, and only this reference keeps a dynamic pool from being evicted by `Router`.
    BackendPoolPtr pool;
    BackendPtr backend;
    bool routed = false;
    bool connection_counted = false;
    bool finished = false;

    ssh_event event = nullptr;
    ssh_channel client_channel = nullptr;

    ssh_session backend_session = nullptr;
    ssh_channel backend_channel = nullptr;
    ssh_key backend_key = nullptr;

    PendingOutput to_backend;
    PendingOutput to_client;
    std::optional<int> backend_exit_status;    /// Forwarded after the output to the client.

    ssh_server_callbacks_struct server_cb{};
    ssh_channel_callbacks_struct client_cb{};
    ssh_channel_callbacks_struct backend_cb{};
};

int onAuthPubkey(ssh_session, const char * user, ssh_key key, char signature_state, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);

    /// libssh calls this twice per offered key: first with `SSH_PUBLICKEY_STATE_NONE` when the client
    /// only asks whether the key is acceptable, then with `SSH_PUBLICKEY_STATE_VALID` once the client
    /// has signed the session with the private key and libssh has verified the signature. Only the
    /// latter proves possession of the key; anything else (a wrong signature) is rejected.
    if (signature_state != SSH_PUBLICKEY_STATE_NONE && signature_state != SSH_PUBLICKEY_STATE_VALID)
        return SSH_AUTH_DENIED;

    /// Clients offer their keys one by one: a key that does not match is denied, and the next one is
    /// evaluated afresh. The route is latched only for the key that authenticated the client.
    if (bridge->routed)
        return SSH_AUTH_DENIED;

    try
    {
        auto public_key = ssh::SSHPublicKey::createNonOwning(key);
        const String canonical = public_key.getType() + " " + public_key.getBase64Representation();

        RouteAttributes attributes;
        attributes.protocol = ListenerProtocol::SSH;
        attributes.user = user ? user : "";
        attributes.authorized_key = canonical;
        attributes.peer_address = bridge->peer.host().toString();

        /// The client only asks whether the key is acceptable and has not proven possession of it yet,
        /// so the answer must have no side effects (a backend template is not expanded into a backend).
        if (signature_state == SSH_PUBLICKEY_STATE_NONE)
        {
            if (bridge->ctx->router.keyIsAcceptable(attributes))
                return SSH_AUTH_SUCCESS;    /// The client proceeds to sign with the key.
            LOG_DEBUG(bridge->log, "SSH {} key (user '{}') is not in the allowlist of a routing rule with an available backend",
                public_key.getType(), attributes.user);
            return SSH_AUTH_DENIED;
        }

        /// A client may send a signed request without asking first, so the allowlist is checked before
        /// the routing, which may expand a backend template into a dynamic backend (and start probing it).
        if (!bridge->ctx->router.keyIsAcceptable(attributes))
        {
            LOG_DEBUG(bridge->log, "SSH {} key (user '{}') is not in the allowlist of a routing rule with an available backend",
                public_key.getType(), attributes.user);
            return SSH_AUTH_DENIED;
        }

        /// Pure routing only: hooks and waits use fibers and must not run inside this callback.
        Router::Decision decision = bridge->ctx->router.routeStatic(attributes, bridge->ctx->listener);

        /// The proxy logs in to the backend with its own bastion key, so the client's key is the only
        /// credential: it must be explicitly listed in the allowlist of the matched rule. A route reached
        /// without one (the listener's default pool, or a rule matching only on the user name, which the
        /// client chooses freely) does not authorize anyone.
        if (!decision.backend || !decision.authorized_key_matched)
        {
            LOG_DEBUG(bridge->log, "SSH {} key (user '{}') is not in the allowlist of a routing rule with an available backend",
                public_key.getType(), attributes.user);
            return SSH_AUTH_DENIED;
        }

        bridge->pool = decision.pool;
        bridge->backend = decision.backend;
        bridge->routed = true;
        LOG_DEBUG(bridge->log, "SSH {} key (user '{}') routed to backend {}",
            public_key.getType(), attributes.user, bridge->backend->name());
        return SSH_AUTH_SUCCESS;
    }
    catch (...)
    {
        /// It is Ok to swallow the error: a failure to route means the key is not authorized.
        return SSH_AUTH_DENIED;
    }
}

/// Writes as much of @p data as the window of @p channel admits, so the write does not wait for the window.
/// Returns the number of bytes written.
size_t writeWithinWindow(ssh_channel channel, const char * data, size_t size, bool is_stderr)
{
    const uint32_t length = static_cast<uint32_t>(std::min<size_t>(size, ssh_channel_window_size(channel)));
    if (length == 0)
        return 0;
    const int written = is_stderr ? ssh_channel_write_stderr(channel, data, length) : ssh_channel_write(channel, data, length);
    return written > 0 ? static_cast<size_t>(written) : 0;
}

/// Forwards data from the source channel to @p destination, keeping what does not fit in its window.
/// Returns the number of bytes consumed from the source.
size_t forward(PendingOutput & out, ssh_channel destination, const char * data, size_t size, bool is_stderr)
{
    size_t consumed = 0;
    if (out.segments.empty())
        consumed = writeWithinWindow(destination, data, size, is_stderr);

    const size_t keep = std::min(size - consumed, max_pending_output_bytes - std::min(out.bytes, max_pending_output_bytes));
    if (keep)
    {
        out.segments.emplace_back(is_stderr, String(data + consumed, keep));
        out.bytes += keep;
        consumed += keep;
    }
    return consumed;
}

/// Called from the splice loop: writes the pending output as the window of @p destination admits, then reads
/// the data left in @p source, and forwards the EOF once everything before it is written.
/// Returns whether everything received from the source has been written.
bool pump(PendingOutput & out, ssh_channel source, ssh_channel destination, bool with_stderr, Backend * backend, bool to_client)
{
    while (!out.segments.empty())
    {
        auto & [is_stderr, data] = out.segments.front();
        const size_t written = writeWithinWindow(destination, data.data(), data.size(), is_stderr);
        out.bytes -= written;
        if (written == data.size())
        {
            out.segments.pop_front();
        }
        else
        {
            data.erase(0, written);
            break;
        }
    }

    bool source_drained = true;
    for (const bool is_stderr : {false, true})
    {
        if (is_stderr && !with_stderr)
            break;
        while (true)
        {
            if (out.bytes >= max_pending_output_bytes)
            {
                source_drained = false;
                break;
            }
            /// The data callbacks may run within the read for newly arrived data: they take the data from
            /// the front of the same buffer, so the order is preserved.
            char buffer[16384];
            const int read = ssh_channel_read_nonblocking(source, buffer,
                static_cast<uint32_t>(std::min(sizeof(buffer), max_pending_output_bytes - out.bytes)), is_stderr);
            if (read <= 0)
                break;
            forward(out, destination, buffer, read, is_stderr);
            if (backend)
            {
                if (to_client)
                    backend->addBytesToClient(read);
                else
                    backend->addBytesFromClient(read);
            }
        }
    }

    const bool drained = source_drained && out.segments.empty();
    if (drained && out.eof_received && !out.eof_sent)
    {
        ssh_channel_send_eof(destination);
        out.eof_sent = true;
    }
    return drained;
}

int onClientData(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel || is_stderr)
        return 0;
    const size_t consumed = forward(bridge->to_backend, bridge->backend_channel, static_cast<const char *>(data), len, false);
    if (bridge->backend && consumed > 0)
        bridge->backend->addBytesFromClient(consumed);
    return static_cast<int>(consumed);
}

void onClientEof(ssh_session, ssh_channel, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    bridge->to_backend.eof_received = true;
}

int onBackendData(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->client_channel)
        return 0;
    const size_t consumed = forward(bridge->to_client, bridge->client_channel, static_cast<const char *>(data), len, is_stderr != 0);
    if (bridge->backend && consumed > 0)
        bridge->backend->addBytesToClient(consumed);
    return static_cast<int>(consumed);
}

void onBackendEof(ssh_session, ssh_channel, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    bridge->to_client.eof_received = true;
}

void onBackendExitStatus(ssh_session, ssh_channel, int exit_status, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    bridge->backend_exit_status = exit_status;
}

int onPtyRequest(ssh_session, ssh_channel, const char * term, int cols, int rows, int /*px*/, int /*py*/, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel)
        return SSH_ERROR;
    return ssh_channel_request_pty_size(bridge->backend_channel, term, cols, rows) == SSH_OK ? SSH_OK : SSH_ERROR;
}

int onPtyWindowChange(ssh_session, ssh_channel, int cols, int rows, int /*px*/, int /*py*/, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel)
        return SSH_ERROR;
    return ssh_channel_change_pty_size(bridge->backend_channel, cols, rows) == SSH_OK ? SSH_OK : SSH_ERROR;
}

int onShellRequest(ssh_session, ssh_channel, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel)
        return SSH_ERROR;
    return ssh_channel_request_shell(bridge->backend_channel) == SSH_OK ? SSH_OK : SSH_ERROR;
}

int onExecRequest(ssh_session, ssh_channel, const char * command, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel)
        return SSH_ERROR;
    return ssh_channel_request_exec(bridge->backend_channel, command) == SSH_OK ? SSH_OK : SSH_ERROR;
}

/// Establish the proxy-to-backend SSH connection (bastion credential) and open a session channel.
bool connectBackend(Bridge * bridge)
{
    const auto & ssh_config = bridge->ctx->config.ssh;
    Backend & backend = *bridge->backend;
    const UInt16 port = backendPortFor(ListenerProtocol::SSH, backend.config(), bridge->ctx->listener.port);

    bridge->backend_session = ssh_new();
    if (!bridge->backend_session)
        return false;

    /// libssh sums `SSH_OPTIONS_TIMEOUT` (whole seconds) and `SSH_OPTIONS_TIMEOUT_USEC` (microseconds),
    /// so both have to be set to honor the millisecond granularity of `connect_timeout_ms`.
    const UInt64 timeout_ms = bridge->ctx->config.connect_timeout_ms;
    const long timeout_sec = static_cast<long>(timeout_ms / 1000);  // NOLINT(google-runtime-int)
    const long timeout_usec = static_cast<long>((timeout_ms % 1000) * 1000);  // NOLINT(google-runtime-int)
    /// The backend's host key is verified against the configured `known_hosts_file` only (never the
    /// user's or the system's files, and no `ssh_config` is read), before the bastion key is used.
    int strict = 1;
    int process_config = 0;
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_PROCESS_CONFIG, &process_config);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_HOST, backend.config().host.c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_PORT_STR, std::to_string(port).c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_USER, ssh_config.backend_user.c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_TIMEOUT, &timeout_sec);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_STRICTHOSTKEYCHECK, &strict);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_KNOWNHOSTS, ssh_config.known_hosts_file.c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_GLOBAL_KNOWNHOSTS, ssh_config.known_hosts_file.c_str());

    /// Feed the same backend-health accounting as `connectToBackend`, so that a dead `ssh_port`
    /// marks the backend down for passive health checks and its connect latency feeds the
    /// `lowest_latency` load balancing, consistently with every other protocol. A failure of any
    /// backend-side step up to opening the session channel fails every session on this backend
    /// (an untrusted host key, a rejected proxy key), so all of them count as connect failures,
    /// and the success is only reported once the backend leg is usable.
    const auto report_failure = [&]
    {
        if (bridge->ctx->router.passiveMarkingDown())
            backend.reportConnectFailure(bridge->ctx->router.failuresToMarkDown());
        else
            backend.reportError();
    };

    const auto started = std::chrono::steady_clock::now();
    if (ssh_connect(bridge->backend_session) != SSH_OK)
    {
        LOG_WARNING(bridge->log, "Cannot connect to SSH backend {}: {}", backend.name(), ssh_get_error(bridge->backend_session));
        report_failure();
        return false;
    }
    const double latency_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();

    /// Fail closed: without a matching host key the peer may be anyone answering at the backend's
    /// address, and it must not receive the proxied session or the bastion's authentication.
    if (const auto known = ssh_session_is_known_server(bridge->backend_session); known != SSH_KNOWN_HOSTS_OK)
    {
        LOG_ERROR(bridge->log, "The host key of SSH backend {} is not trusted (status {}, {}); add it to {}",
            backend.name(), static_cast<int>(known), ssh_get_error(bridge->backend_session), ssh_config.known_hosts_file);
        report_failure();
        return false;
    }

    if (ssh_pki_import_privkey_file(ssh_config.backend_key_file.c_str(), nullptr, nullptr, nullptr, &bridge->backend_key) != SSH_OK)
    {
        /// A proxy-local configuration problem: do not blame the backend for it.
        LOG_ERROR(bridge->log, "Cannot load the proxy backend key from {}", ssh_config.backend_key_file);
        return false;
    }

    if (ssh_userauth_publickey(bridge->backend_session, nullptr, bridge->backend_key) != SSH_AUTH_SUCCESS)
    {
        LOG_WARNING(bridge->log, "Backend {} rejected the proxy key: {}", backend.name(), ssh_get_error(bridge->backend_session));
        report_failure();
        return false;
    }

    bridge->backend_channel = ssh_channel_new(bridge->backend_session);
    if (!bridge->backend_channel || ssh_channel_open_session(bridge->backend_channel) != SSH_OK)
    {
        LOG_WARNING(bridge->log, "Cannot open a session channel on backend {}", backend.name());
        report_failure();
        return false;
    }
    backend.reportConnectSuccess(latency_ms);

    bridge->backend_cb.userdata = bridge;
    bridge->backend_cb.channel_data_function = onBackendData;
    bridge->backend_cb.channel_eof_function = onBackendEof;
    bridge->backend_cb.channel_exit_status_function = onBackendExitStatus;
    ssh_callbacks_init(&bridge->backend_cb) // NOLINT: macro statement, no trailing semicolon
    ssh_set_channel_callbacks(bridge->backend_channel, &bridge->backend_cb);
    ssh_event_add_session(bridge->event, bridge->backend_session);
    return true;
}

ssh_channel onChannelOpen(ssh_session session, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (bridge->client_channel || !bridge->backend)
        return nullptr;

    /// Connect to the backend first so its channel exists before the client sends pty/shell/exec.
    if (!connectBackend(bridge))
    {
        bridge->finished = true;
        return nullptr;
    }

    bridge->client_channel = ssh_channel_new(session);
    if (!bridge->client_channel)
        return nullptr;

    bridge->client_cb.userdata = bridge;
    bridge->client_cb.channel_data_function = onClientData;
    bridge->client_cb.channel_eof_function = onClientEof;
    bridge->client_cb.channel_pty_request_function = onPtyRequest;
    bridge->client_cb.channel_pty_window_change_function = onPtyWindowChange;
    bridge->client_cb.channel_shell_request_function = onShellRequest;
    bridge->client_cb.channel_exec_request_function = onExecRequest;
    ssh_callbacks_init(&bridge->client_cb) // NOLINT: macro statement, no trailing semicolon
    ssh_set_channel_callbacks(bridge->client_channel, &bridge->client_cb);

    bridge->backend->onConnectionStart();
    bridge->connection_counted = true;
    return bridge->client_channel;
}

void runSSHSession(int owned_fd, const FrontendContext & ctx)
{
    SCOPE_EXIT({ [[maybe_unused]] int err = ::close(owned_fd); });

    ssh::SSHBind bind;
    bind.disableDefaultConfig();
    bind.setHostKey(ctx.config.ssh.host_key_file);
    bind.setBanner(ctx.config.ssh.banner);

    ssh::SSHSession session;
    session.disableDefaultConfig();

    /// libssh takes ownership of the fd it accepts, so hand it a duplicate and keep closing the
    /// original ourselves (via the scope guard above). The duplicate stays close-on-exec, so hooks do not inherit it.
    int accept_fd = ::fcntl(owned_fd, F_DUPFD_CLOEXEC, 0);
    if (accept_fd < 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot duplicate the SSH socket fd");
    bind.acceptFd(session, accept_fd);

    Bridge bridge;
    bridge.ctx = &ctx;
    bridge.log = ctx.log;

    sockaddr_storage addr{};
    socklen_t addr_len = sizeof(addr);
    if (::getpeername(owned_fd, reinterpret_cast<sockaddr *>(&addr), &addr_len) == 0)
        bridge.peer = Poco::Net::SocketAddress(reinterpret_cast<const sockaddr *>(&addr), addr_len);

    bridge.server_cb.userdata = &bridge;
    bridge.server_cb.auth_pubkey_function = onAuthPubkey;
    bridge.server_cb.channel_open_request_session_function = onChannelOpen;
    ssh_callbacks_init(&bridge.server_cb) // NOLINT: macro statement, no trailing semicolon
    ssh_set_auth_methods(session.getInternalPtr(), SSH_AUTH_METHOD_PUBLICKEY);
    ssh_set_server_callbacks(session.getInternalPtr(), &bridge.server_cb);

    /// Bound the blocking key exchange, so a client that stalls before completing it cannot hold the
    /// borrowed thread for long. libssh sums the whole seconds and the microseconds.
    const UInt64 handshake_timeout_ms = ctx.config.handshake_timeout_ms;
    const long handshake_timeout_sec = static_cast<long>(handshake_timeout_ms / 1000);  // NOLINT(google-runtime-int)
    const long handshake_timeout_usec = static_cast<long>((handshake_timeout_ms % 1000) * 1000);  // NOLINT(google-runtime-int)
    if (ssh_options_set(session.getInternalPtr(), SSH_OPTIONS_TIMEOUT, &handshake_timeout_sec) != SSH_OK
        || ssh_options_set(session.getInternalPtr(), SSH_OPTIONS_TIMEOUT_USEC, &handshake_timeout_usec) != SSH_OK)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot set the SSH handshake timeout");

    session.handleKeyExchange();

    bridge.event = ssh_event_new();
    SCOPE_EXIT({ if (bridge.event) ssh_event_free(bridge.event); });
    ssh_event_add_session(bridge.event, session.getInternalPtr());

    const int poll_ms = 200;
    const int max_iterations = static_cast<int>(ctx.config.ssh.auth_timeout_ms / poll_ms) + 1;

    for (int iterations = 0; !bridge.client_channel && !bridge.finished && iterations < max_iterations; ++iterations)
    {
        if (ssh_event_dopoll(bridge.event, poll_ms) == SSH_ERROR)
        {
            bridge.finished = true;
            break;
        }
    }

    /// Splice the two channels until either side closes, and the output of a closed backend is delivered.
    while (!bridge.finished && bridge.client_channel && ssh_channel_is_open(bridge.client_channel) && bridge.backend_channel)
    {
        const bool backend_open = ssh_channel_is_open(bridge.backend_channel);
        if (backend_open)
            pump(bridge.to_backend, bridge.client_channel, bridge.backend_channel, /*with_stderr=*/ false, bridge.backend.get(), /*to_client=*/ false);
        const bool to_client_drained = pump(bridge.to_client, bridge.backend_channel, bridge.client_channel,
            /*with_stderr=*/ true, bridge.backend.get(), /*to_client=*/ true);

        if (to_client_drained && bridge.backend_exit_status)
        {
            ssh_channel_request_send_exit_status(bridge.client_channel, *bridge.backend_exit_status);
            bridge.backend_exit_status.reset();
        }
        if (!backend_open && to_client_drained)
            break;

        if (ssh_event_dopoll(bridge.event, poll_ms) == SSH_ERROR)
            break;
    }

    if (bridge.connection_counted && bridge.backend)
        bridge.backend->onConnectionEnd();

    if (bridge.backend_channel)
    {
        ssh_channel_close(bridge.backend_channel);
        ssh_channel_free(bridge.backend_channel);
    }
    if (bridge.backend_key)
        ssh_key_free(bridge.backend_key);
    if (bridge.backend_session)
    {
        ssh_disconnect(bridge.backend_session);
        ssh_free(bridge.backend_session);
    }
    if (bridge.client_channel)
    {
        ssh_channel_close(bridge.client_channel);
        ssh_channel_free(bridge.client_channel);
    }
    session.disconnect();
}

}

void handleSSH(int fd, const FrontendContext & ctx)
{
    if (ctx.config.ssh.backend_key_file.empty())
    {
        LOG_ERROR(ctx.log, "An SSH listener requires <proxy><ssh><backend_key_file> to authenticate to backends");
        [[maybe_unused]] int err = ::close(fd);
        return;
    }

    static const size_t max_sessions = [&]
    {
        if (ctx.config.ssh.max_sessions)
            return static_cast<size_t>(ctx.config.ssh.max_sessions);
        /// The scheduler has a blocking-work thread per CPU it runs on.
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        size_t cpu_count = 1;
        if (0 == sched_getaffinity(0, sizeof(cpus), &cpus))
            cpu_count = CPU_COUNT(&cpus);
        return std::max<size_t>(1, cpu_count / 2);
    }();
    static std::atomic<size_t> active_sessions{0};

    if (active_sessions.fetch_add(1, std::memory_order_relaxed) >= max_sessions)
    {
        active_sessions.fetch_sub(1, std::memory_order_relaxed);
        LOG_WARNING(ctx.log, "Rejecting an SSH connection: the limit of {} concurrent SSH sessions "
            "(<proxy><ssh><max_sessions>) is reached", max_sessions);
        [[maybe_unused]] int err = ::close(fd);
        return;
    }
    SCOPE_EXIT({ active_sessions.fetch_sub(1, std::memory_order_relaxed); });

    /// Interactive traffic is latency-sensitive; disable Nagle's algorithm on the client fd.
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    try
    {
        /// libssh drives its own blocking I/O and poll loop, so run it outside the cooperative
        /// scheduler on the borrowed OS thread for the lifetime of the connection.
        silk::FiberScheduler::ThreadModeScope thread_mode;
        runSSHSession(fd, ctx);
    }
    catch (...)
    {
        LOG_DEBUG(ctx.log, "SSH connection failed: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
    }
}

void validateSSHKeys(const ProxyConfiguration & config)
{
    ssh_key key = nullptr;
    if (ssh_pki_import_privkey_file(config.ssh.host_key_file.c_str(), nullptr, nullptr, nullptr, &key) != SSH_OK)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Cannot load the SSH host key from '{}' specified in <proxy><ssh><host_key_file>", config.ssh.host_key_file);
    ssh_key_free(key);

    key = nullptr;
    if (ssh_pki_import_privkey_file(config.ssh.backend_key_file.c_str(), nullptr, nullptr, nullptr, &key) != SSH_OK)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Cannot load the SSH backend key from '{}' specified in <proxy><ssh><backend_key_file>", config.ssh.backend_key_file);
    ssh_key_free(key);

    if (::access(config.ssh.known_hosts_file.c_str(), R_OK) != 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Cannot read the SSH backend host keys from '{}' specified in <proxy><ssh><known_hosts_file>", config.ssh.known_hosts_file);
}

#else

void handleSSH(int fd, const FrontendContext & ctx)
{
    LOG_ERROR(ctx.log, "SSH proxying requires a build with libssh (USE_SSH) on Linux");
    [[maybe_unused]] int err = ::close(fd);
}

void validateSSHKeys(const ProxyConfiguration &)
{
    /// Unreachable: the config loader rejects 'ssh' listeners on such builds.
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSH proxying requires a build with libssh (USE_SSH) on Linux");
}

#endif

}

#endif
