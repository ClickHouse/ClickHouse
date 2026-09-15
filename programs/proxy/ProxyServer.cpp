#include <ProxyServer.h>

#if USE_SILK

#include <Relay.h>
#include <SocketIO.h>
#include <TLSSupport.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/init.h>

#include <base/errnoToString.h>

#include <cerrno>
#include <sys/socket.h>

#if USE_SSL
#include <Interpreters/Context.h>
#include <Server/ACME/Client.h>
#include <Poco/Net/Context.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace DB::Proxy
{

namespace
{

/// Trivially copyable parameters for the accept-loop fiber (silk copies them into the fiber).
struct AcceptParams
{
    int listen_fd;
    const FrontendContext * ctx;
    std::atomic<bool> * stopped;
};

void dispatch(FiberSocket & client, const FrontendContext & ctx)
{
    switch (ctx.listener.protocol)
    {
        case ListenerProtocol::HTTP: handleHTTP(client, ctx); break;
        case ListenerProtocol::Native: handleNative(client, ctx); break;
        case ListenerProtocol::MySQL: handleMySQL(client, ctx); break;
        case ListenerProtocol::PostgreSQL: handlePostgreSQL(client, ctx); break;
        case ListenerProtocol::TLS:
        case ListenerProtocol::Stream: handlePassthrough(client, ctx); break;
        case ListenerProtocol::SSH: break;    /// Handled before dispatch, on the raw fd.
    }
}

struct HandlerParams
{
    int fd;
    const FrontendContext * ctx;
};

int connectionFiber(HandlerParams * params) noexcept
{
    const FrontendContext & ctx = *params->ctx;
    try
    {
        if (ctx.listener.protocol == ListenerProtocol::SSH)
        {
            /// SSH is terminated with libssh, which owns the raw fd and drives its own I/O.
            handleSSH(params->fd, ctx);
            return 0;
        }

        FiberSocket client;
        if (ctx.listener.secure)
        {
#if USE_SSL
            client = FiberSocket::adoptTLS(params->fd, ctx.server_tls_context);
#else
            [[maybe_unused]] int err = ::close(params->fd);
            return 0;
#endif
        }
        else
        {
            client = FiberSocket::adopt(params->fd);
        }

        dispatch(client, ctx);
    }
    catch (...)
    {
        LOG_DEBUG(ctx.log, "Connection handler failed: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
    }
    return 0;
}

/// Spawn a detached connection handler: the future deletes itself once the fiber completes.
void spawnConnection(int fd, const FrontendContext * ctx)
{
    auto * future = new silk::FiberFuture;
    int run_result = silk::FiberScheduler::run(connectionFiber, HandlerParams{fd, ctx}, future);
    if (run_result != 0)
    {
        LOG_WARNING(ctx->log, "Cannot allocate a fiber for a new connection; dropping it");
        delete future;
        [[maybe_unused]] int err = ::close(fd);
        return;
    }
    if (!future->subscribe(+[](silk::FiberFuture * f) noexcept { delete f; }))
        delete future;    /// The handler already finished; clean up now.
}

/// Errors that persist until some resource is released. Retrying them without a pause busy-spins a scheduler thread.
bool isResourceExhaustion(int err)
{
    return err == EMFILE || err == ENFILE || err == ENOMEM || err == ENOBUFS;
}

int acceptLoop(AcceptParams * params) noexcept
{
    /// Pause before retrying `accept` after a resource-exhaustion error, so the listener does not busy-spin.
    static constexpr UInt64 resource_backoff_ms = 100;

    while (!params->stopped->load(std::memory_order_relaxed))
    {
        uint64_t client_fd = 0;
        int r = silk::FiberScheduler::accept(params->listen_fd, nullptr, nullptr, SOCK_CLOEXEC, &client_fd);
        if (r != 0)
        {
            if (params->stopped->load(std::memory_order_relaxed))
                break;
            if (isResourceExhaustion(r))
            {
                LOG_WARNING(
                    params->ctx->log,
                    "Cannot accept a connection: {}. Pausing the listener for {} ms.",
                    errnoToString(r),
                    resource_backoff_ms);
                silk::FiberScheduler::sleep(resource_backoff_ms * 1'000'000);
                continue;
            }
            /// Transient error (e.g. the peer aborted before accept): keep listening.
            continue;
        }
        spawnConnection(static_cast<int>(client_fd), params->ctx);
    }
    return 0;
}

}

/// Owns a running accept loop: the parameters copied into its fiber and the future it completes on.
struct ProxyServer::AcceptState
{
    AcceptParams params {};
    silk::FiberFuture future;
};

ProxyServer::ProxyServer(const Poco::Util::AbstractConfiguration & abstract_config, LoggerPtr log_)
    : config(ProxyConfiguration::load(abstract_config))
    , log(log_)
{
}

ProxyServer::~ProxyServer()
{
    stop();
}

bool ProxyServer::anySecureListener() const
{
    for (const auto & listener : config.listeners)
        if (listener.secure)
            return true;
    return false;
}

bool ProxyServer::anyBackendSecure() const
{
    for (const auto & [_, pool] : config.pools)
        for (const auto & backend : pool.backends)
            if (backend.secure)
                return true;
    /// Backends materialized from a `backend_template` are not part of any pool, but they need
    /// the client TLS context just the same, and it can only be created before the scheduler starts.
    for (const auto & rule : config.rules)
        if (rule.backend_template && rule.backend_template->secure)
            return true;
    return false;
}

void ProxyServer::bindAndListen()
{
    for (const auto & listener : config.listeners)
    {
        const String host = listener.host.empty() ? config.listen_host : listener.host;
        auto socket = std::make_unique<Poco::Net::ServerSocket>();
        socket->bind(Poco::Net::SocketAddress(host, listener.port), /*reuseAddress=*/ true);
        socket->listen(config.listen_backlog);
        LOG_INFO(log, "Listening for {} on {}:{}{}",
            toString(listener.protocol), host, listener.port, listener.secure ? " (TLS)" : "");
        listen_sockets.push_back(std::move(socket));
    }
}

void ProxyServer::start(const Poco::Util::AbstractConfiguration & abstract_config)
{
#if USE_SSL
    if (anySecureListener())
        server_tls_context = makeServerTLSContext(abstract_config);
    if (anyBackendSecure())
        client_tls_context = makeClientTLSContext(abstract_config);

    if (abstract_config.has("acme"))
    {
        /// ACME needs a global context (for ZooKeeper coordination and the background refresh tasks).
        /// The challenge is served by the HTTP frontend at /.well-known/acme-challenge/.
        /// These are kept for the lifetime of the process on purpose.
        static auto shared_context = Context::createShared();
        static auto global_context = Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        global_context->setApplicationType(Context::ApplicationType::SERVER);
        ACME::Client::instance().initialize(abstract_config);
        acme_enabled = true;
        LOG_INFO(log, "ACME certificate provisioning is enabled");
    }
#else
    if (anySecureListener() || anyBackendSecure() || abstract_config.has("acme"))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS features require a build with SSL support");
#endif

    /// A broken SSH credential must fail startup, not every accepted connection.
    for (const auto & listener : config.listeners)
    {
        if (listener.protocol == ListenerProtocol::SSH)
        {
            validateSSHKeys(config);
            break;
        }
    }

    /// Bind before starting the scheduler so a bind failure fails startup cleanly.
    bindAndListen();

    router = std::make_unique<Router>(config, config.health_check.enabled);
    health = std::make_unique<HealthMonitor>(config, *router);

    /// One shared, read-only handler context per listener.
    for (const auto & listener : config.listeners)
    {
#if USE_SSL
        auto ctx = std::make_unique<FrontendContext>(FrontendContext{
            .config = config,
            .listener = listener,
            .router = *router,
            .server_tls_context = server_tls_context,
            .client_tls_context = client_tls_context,
            .log = log,
        });
#else
        auto ctx = std::make_unique<FrontendContext>(FrontendContext{
            .config = config,
            .listener = listener,
            .router = *router,
            .log = log,
        });
#endif
        frontend_contexts.push_back(std::move(ctx));
    }

    /// Start the fiber scheduler. OpenSSL handshakes run on fiber stacks and need extra room.
    silk::initialize();
    silk::FiberScheduler::Options options;
    options.fiberStackSize = config.fiber_stack_size;
    silk::FiberScheduler::initialize(&options);
    silk_initialized = true;

    health->start();

    for (size_t i = 0; i < listen_sockets.size(); ++i)
    {
        auto state = std::make_unique<AcceptState>();
        state->params = AcceptParams{listen_sockets[i]->sockfd(), frontend_contexts[i].get(), &stopped};

        AcceptState * raw = state.get();
        if (silk::FiberScheduler::run(acceptLoop, AcceptParams(raw->params), &raw->future) != 0)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot start the accept loop fiber");
        accept_states.push_back(std::move(state));
    }

    LOG_INFO(log, "Proxy started with {} listeners", listen_sockets.size());
}

void ProxyServer::stop()
{
    if (stopped.exchange(true))
        return;

    if (health)
        health->stop();

    /// Unblock the accept loops by shutting down and closing the listening sockets.
    for (auto & socket : listen_sockets)
    {
        try
        {
            ::shutdown(socket->sockfd(), SHUT_RDWR);
            socket->close();
        }
        catch (...)  // NOLINT(bugprone-empty-catch)
        {
            /// The socket may already be closed; that is Ok, we are shutting down anyway.
        }
    }

    /// Wait for the accept loops to observe the stop flag and finish.
    for (auto & state : accept_states)
        state->future.wait();

    LOG_INFO(log, "Proxy stopped accepting connections");

    /// Join the health supervisor so no fiber remains scheduled, then stop the scheduler threads.
    /// Doing this before returning avoids a race between the still-running scheduler threads and
    /// process teardown. In-flight client connections are dropped.
    if (health)
        health->join();

#if USE_SSL
    if (acme_enabled)
    {
        ACME::Client::instance().shutdown();
        acme_enabled = false;
    }
#endif

    if (silk_initialized)
    {
        silk::FiberScheduler::destroy();
        silk::destroy();
        silk_initialized = false;
    }
}

}

#endif
