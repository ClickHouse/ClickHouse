#include <Frontend.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <silk/fibers/fiber.h>

#include <base/scope_guard.h>

#include <Poco/Net/SocketAddress.h>

#include <chrono>
#include <string>

#include <netinet/in.h>
#include <netinet/tcp.h>
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
struct Bridge
{
    const FrontendContext * ctx = nullptr;
    Poco::Net::SocketAddress peer;
    LoggerPtr log;

    BackendPtr backend;
    bool routed = false;
    bool connection_counted = false;
    bool finished = false;

    ssh_event event = nullptr;
    ssh_channel client_channel = nullptr;

    ssh_session backend_session = nullptr;
    ssh_channel backend_channel = nullptr;
    ssh_key backend_key = nullptr;

    ssh_server_callbacks_struct server_cb{};
    ssh_channel_callbacks_struct client_cb{};
    ssh_channel_callbacks_struct backend_cb{};
};

int onAuthPubkey(ssh_session, const char * user, ssh_key key, char /*signature_state*/, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    try
    {
        auto public_key = ssh::SSHPublicKey::createNonOwning(key);
        const String canonical = public_key.getType() + " " + public_key.getBase64Representation();

        if (!bridge->routed)
        {
            RouteAttributes attributes;
            attributes.protocol = ListenerProtocol::SSH;
            attributes.user = user ? user : "";
            attributes.authorized_key = canonical;
            attributes.peer_address = bridge->peer.host().toString();

            /// Pure routing only: hooks and waits use fibers and must not run inside this callback.
            bridge->backend = bridge->ctx->router.routeStatic(attributes, bridge->ctx->listener).backend;
            bridge->routed = true;

            if (bridge->backend)
                LOG_DEBUG(bridge->log, "SSH {} key (user '{}') routed to backend {}",
                    public_key.getType(), attributes.user, bridge->backend->name());
            else
                LOG_WARNING(bridge->log, "SSH {} key (user '{}') matched no backend; rejecting",
                    public_key.getType(), attributes.user);
        }
    }
    catch (...)
    {
        /// It is Ok to swallow the error: a failure to route means the key is not authorized.
        return SSH_AUTH_DENIED;
    }

    /// The client is authorized by the fact that its key selects a backend; the real signature check
    /// happens on the re-originated connection to that backend.
    return bridge->backend ? SSH_AUTH_SUCCESS : SSH_AUTH_DENIED;
}

int onClientData(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel || is_stderr)
        return 0;
    int written = ssh_channel_write(bridge->backend_channel, data, len);
    if (bridge->backend && written > 0)
        bridge->backend->addBytesFromClient(written);
    return written > 0 ? written : 0;
}

void onClientEof(ssh_session, ssh_channel, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (bridge->backend_channel)
        ssh_channel_send_eof(bridge->backend_channel);
}

int onBackendData(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->client_channel)
        return 0;
    int written = is_stderr
        ? ssh_channel_write_stderr(bridge->client_channel, data, len)
        : ssh_channel_write(bridge->client_channel, data, len);
    if (bridge->backend && written > 0)
        bridge->backend->addBytesToClient(written);
    return written > 0 ? written : 0;
}

void onBackendEof(ssh_session, ssh_channel, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (bridge->client_channel)
        ssh_channel_send_eof(bridge->client_channel);
}

void onBackendExitStatus(ssh_session, ssh_channel, int exit_status, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (bridge->client_channel)
        ssh_channel_request_send_exit_status(bridge->client_channel, exit_status);
}

int onPtyRequest(ssh_session, ssh_channel, const char * term, int cols, int rows, int /*px*/, int /*py*/, void * userdata)
{
    auto * bridge = static_cast<Bridge *>(userdata);
    if (!bridge->backend_channel)
        return SSH_ERROR;
    return ssh_channel_request_pty_size(bridge->backend_channel, term, cols, rows) == SSH_OK ? SSH_OK : SSH_ERROR;
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
    int no_strict = 0;
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_HOST, backend.config().host.c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_PORT_STR, std::to_string(port).c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_USER, ssh_config.backend_user.c_str());
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_TIMEOUT, &timeout_sec);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
    ssh_options_set(bridge->backend_session, SSH_OPTIONS_STRICTHOSTKEYCHECK, &no_strict);

    /// Feed the same backend-health accounting as `connectToBackend`, so that a dead `ssh_port`
    /// marks the backend down for passive health checks and its connect latency feeds the
    /// `lowest_latency` load balancing, consistently with every other protocol.
    const auto started = std::chrono::steady_clock::now();
    if (ssh_connect(bridge->backend_session) != SSH_OK)
    {
        LOG_WARNING(bridge->log, "Cannot connect to SSH backend {}: {}", backend.name(), ssh_get_error(bridge->backend_session));
        if (bridge->ctx->router.passiveMarkingDown())
            backend.reportConnectFailure(bridge->ctx->router.failuresToMarkDown());
        else
            backend.reportError();
        return false;
    }
    const double latency_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
    backend.reportConnectSuccess(latency_ms);

    if (ssh_pki_import_privkey_file(ssh_config.backend_key_file.c_str(), nullptr, nullptr, nullptr, &bridge->backend_key) != SSH_OK)
    {
        /// A proxy-local configuration problem: do not blame the backend for it.
        LOG_ERROR(bridge->log, "Cannot load the proxy backend key from {}", ssh_config.backend_key_file);
        return false;
    }

    if (ssh_userauth_publickey(bridge->backend_session, nullptr, bridge->backend_key) != SSH_AUTH_SUCCESS)
    {
        LOG_WARNING(bridge->log, "Backend {} rejected the proxy key: {}", backend.name(), ssh_get_error(bridge->backend_session));
        backend.reportError();
        return false;
    }

    bridge->backend_channel = ssh_channel_new(bridge->backend_session);
    if (!bridge->backend_channel || ssh_channel_open_session(bridge->backend_channel) != SSH_OK)
    {
        LOG_WARNING(bridge->log, "Cannot open a session channel on backend {}", backend.name());
        backend.reportError();
        return false;
    }

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
    /// original ourselves (via the scope guard above).
    int accept_fd = ::dup(owned_fd);
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

    /// Splice the two channels until either side closes.
    while (!bridge.finished && bridge.client_channel && ssh_channel_is_open(bridge.client_channel)
        && bridge.backend_channel && ssh_channel_is_open(bridge.backend_channel))
    {
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
