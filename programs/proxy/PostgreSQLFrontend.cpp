#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <optional>
#include <vector>


namespace DB::Proxy
{

namespace
{

constexpr UInt32 SSL_REQUEST = 80877103;
constexpr UInt32 GSSENC_REQUEST = 80877104;
constexpr UInt32 CANCEL_REQUEST = 80877102;
constexpr UInt32 PROTOCOL_VERSION_3 = 196608;

/// Parse the "user" and "database" parameters out of a PostgreSQL StartupMessage parameter block
/// (a sequence of NUL-terminated key/value strings, then a terminating NUL), exactly as the server's
/// `StartupMessage::deserialize` does: pairs are read up to the terminating byte, including pairs with
/// an empty key, and a repeated parameter takes the last value. Returns false for a block the server rejects,
/// so that the proxy never routes by parameters other than the ones the backend authenticates with.
bool parseStartupParameters(const String & params, RouteAttributes & attributes)
{
    if (params.empty() || params.back() != '\0')
        return false;

    const size_t terminator = params.size() - 1;
    size_t i = 0;
    while (i < terminator)
    {
        const size_t key_end = params.find('\0', i);
        if (key_end >= terminator)
            return false;
        const size_t value_end = params.find('\0', key_end + 1);
        if (value_end >= terminator)
            return false;

        const std::string_view key(params.data() + i, key_end - i);
        const std::string_view value(params.data() + key_end + 1, value_end - key_end - 1);
        i = value_end + 1;

        if (key == "user")
            attributes.user = value;
        else if (key == "database")
            attributes.database = value;
    }

    /// The server does not default an omitted or empty database to the user name (unlike PostgreSQL):
    /// the session keeps the user's default database, which the proxy does not know, so it stays empty.
    return true;
}

/// The delivery of a `CancelRequest` to one backend, run on its own fiber.
struct CancelDelivery
{
    const FrontendContext * ctx;
    Backend * backend;
    const String * packet;
    UInt64 timeout_ms;
    std::atomic<size_t> * delivered;
};

int deliverCancelRequest(CancelDelivery * task) noexcept
{
    const FrontendContext & ctx = *task->ctx;
    try
    {
        const UInt16 port = backendPortFor(ListenerProtocol::PostgreSQL, task->backend->config(), ctx.listener.port);
        FiberSocket socket = FiberSocket::connect(resolveAddress(task->backend->config().host, port), task->timeout_ms);
        socket.setTimeouts(task->timeout_ms, task->timeout_ms);
        socket.sendAll(task->packet->data(), task->packet->size());
        socket.close();
        task->delivered->fetch_add(1, std::memory_order_relaxed);
    }
    catch (...)
    {
        /// Not a health signal (the request is sent to backends that may not serve this session):
        /// only log it. The other backends still receive the request.
        LOG_DEBUG(ctx.log, "Cannot deliver a PostgreSQL CancelRequest to backend {}: {}", task->backend->name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
    }
    return 0;
}

/// A `CancelRequest` arrives on a new connection and must reach the backend serving the session it
/// cancels. The proxy does not track sessions (their `BackendKeyData` passes through the relay unparsed),
/// but the key is a random secret: deliver the request to every backend the listener can route to, and
/// only the one that issued the key acts on it. The protocol has no response to a `CancelRequest`.
///
/// The request is not authenticated, and each unreachable backend costs a connect timeout. So the backends
/// receive the request concurrently, in batches, for an unreachable backend not to delay the others,
/// the whole broadcast has the time budget of a handshake, and it stops when the proxy shuts down.
/// The backends that are not marked down go first: they most likely serve the session.
void broadcastCancelRequest(const String & packet, const FrontendContext & ctx)
{
    constexpr size_t max_concurrent_deliveries = 64;

    std::vector<BackendPtr> backends = ctx.router.reachableBackends(ctx.listener);
    /// A secure backend cannot be reached over this protocol (see `connectToBackend`). A backend that
    /// is marked down still receives the request: `alive` only gates new routing, and the sessions that
    /// were relayed to it before it was marked down may still be running there.
    std::erase_if(backends, [](const BackendPtr & backend) { return backend->config().secure; });
    std::stable_partition(backends.begin(), backends.end(), [](const BackendPtr & backend) { return backend->isAlive(); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ctx.config.handshake_timeout_ms);
    std::atomic<size_t> delivered = 0;
    size_t attempted = 0;
    while (attempted < backends.size())
    {
        const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (remaining_ms <= 0 || relaysAborted())
            break;
        const UInt64 timeout_ms = std::min<UInt64>(ctx.config.connect_timeout_ms, remaining_ms);

        const size_t batch_size = std::min(backends.size() - attempted, max_concurrent_deliveries);
        std::vector<silk::FiberFuture> futures(batch_size);
        size_t started = 0;
        /// The fibers use `packet`, `ctx`, `delivered` and the backends, so they are joined before leaving the scope.
        SCOPE_EXIT({
            for (size_t i = 0; i < started; ++i)
                futures[i].wait();
        });
        for (size_t i = 0; i < batch_size; ++i)
        {
            CancelDelivery task{&ctx, backends[attempted + i].get(), &packet, timeout_ms, &delivered};
            if (silk::FiberScheduler::run(deliverCancelRequest, CancelDelivery(task), &futures[started]) == 0)
                ++started;
            else
                deliverCancelRequest(&task);    /// No fiber is available: deliver from this one.
        }
        attempted += batch_size;
    }

    if (attempted < backends.size())
        LOG_WARNING(ctx.log, "Delivered a PostgreSQL CancelRequest to {} backends; {} backends were skipped, because {}",
            delivered.load(), backends.size() - attempted,
            relaysAborted() ? "the proxy is shutting down" : "the time budget of the request is used up");
    else
        LOG_DEBUG(ctx.log, "Delivered a PostgreSQL CancelRequest to {} backends", delivered.load());
}

}

void handlePostgreSQL(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::PostgreSQL;
    attributes.peer_address = client.peerAddress().host().toString();

    /// When a rule or a first-seen hook needs the user or the database, the proxy must read them from
    /// the cleartext `StartupMessage`, so it declines a request to encrypt the connection: the client then
    /// either continues in cleartext or, if it requires encryption, disconnects. Routing such a connection by
    /// the default pool instead would silently ignore the rules, and never run the hooks.
    const bool needs_credentials = ctx.router.needsCredentials(ListenerProtocol::PostgreSQL) || ctx.router.hasFirstSeenHooks();

    std::optional<RecordingReader> reader;
    try
    {
        while (true)
        {
            reader.emplace(client);
            const UInt32 length = reader->readBE<UInt32>();
            const UInt32 code = reader->readBE<UInt32>();

            if (code == PROTOCOL_VERSION_3)
            {
                /// A cleartext StartupMessage: read its parameters to route by user and database.
                if (length < 8 || length > 1024 * 1024)
                {
                    LOG_WARNING(ctx.log, "Invalid PostgreSQL StartupMessage length {}", length);
                    return;
                }
                const String params = reader->readFixed(length - 8);
                if (!parseStartupParameters(params, attributes))
                {
                    LOG_WARNING(ctx.log, "Invalid PostgreSQL StartupMessage parameters");
                    return;
                }
                break;
            }

            if (code == CANCEL_REQUEST)
            {
                if (length != 16)
                {
                    LOG_WARNING(ctx.log, "Invalid PostgreSQL CancelRequest length {}", length);
                    return;
                }
                reader->readFixed(8);   /// The process ID and the secret key.
                broadcastCancelRequest(reader->received(), ctx);
                return;
            }

            if (code == SSL_REQUEST || code == GSSENC_REQUEST)
            {
                if (needs_credentials)
                {
                    /// The client waits for the answer before sending anything else.
                    if (reader->buffered() != 0)
                    {
                        LOG_WARNING(ctx.log, "Unexpected data after a PostgreSQL encryption request");
                        return;
                    }
                    const char no = 'N';
                    client.sendAll(&no, 1);
                    continue;
                }

                /// No rule or hook needs the user or the database: route by peer address or the default pool
                /// and forward the bytes verbatim, so the backend negotiates encryption end to end.
                /// The proxy cannot see an encrypted `CancelRequest` to broadcast it, so choose the backend
                /// by the client address: the cancellation connection of an encrypted session, which
                /// keeps its encryption, then reaches the backend serving that session.
                attributes.force_sticky_by_peer_address = true;
                LOG_DEBUG(ctx.log, "PostgreSQL connection begins with an encryption request ({}); "
                    "routing by peer address or the default pool", code);
                break;
            }

            LOG_WARNING(ctx.log, "Unknown leading PostgreSQL message code {}", code);
            return;
        }
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot parse the PostgreSQL startup: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return;
    }

    RouteResult route = routeConnection(ctx, attributes);
    if (!route.backend)
        return;

    Backend & backend = *route.backend;
    backend.onConnectionStart();
    SCOPE_EXIT({ backend.onConnectionEnd(); });

    FiberSocket backend_socket;
    try
    {
        backend_socket = connectToBackend(ctx, backend, backend.config().secure);
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot connect to backend {}: {}", backend.name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return;
    }

    LOG_DEBUG(ctx.log, "Routing PostgreSQL connection (user='{}', database='{}') to backend {}",
        attributes.user, attributes.database, backend.name());

    runRelay(client, backend_socket, &backend, reader->received(), ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

#endif
