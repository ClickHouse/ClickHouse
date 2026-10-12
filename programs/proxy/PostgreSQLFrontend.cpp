#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>

#include <optional>


namespace DB::Proxy
{

namespace
{

constexpr UInt32 SSL_REQUEST = 80877103;
constexpr UInt32 GSSENC_REQUEST = 80877104;
constexpr UInt32 CANCEL_REQUEST = 80877102;
constexpr UInt32 PROTOCOL_VERSION_3 = 196608;

/// Parse the "user" and "database" parameters out of a PostgreSQL StartupMessage parameter block
/// (a sequence of NUL-terminated key/value strings).
void parseStartupParameters(const String & params, RouteAttributes & attributes)
{
    size_t i = 0;
    while (i < params.size())
    {
        const size_t key_end = params.find('\0', i);
        if (key_end == String::npos)
            break;
        const String key = params.substr(i, key_end - i);
        if (key.empty())
            break;

        const size_t value_end = params.find('\0', key_end + 1);
        if (value_end == String::npos)
            break;
        const String value = params.substr(key_end + 1, value_end - key_end - 1);
        i = value_end + 1;

        if (key == "user")
            attributes.user = value;
        else if (key == "database")
            attributes.database = value;
    }

    /// PostgreSQL defaults the database to the user name when it is not given explicitly.
    if (attributes.database.empty())
        attributes.database = attributes.user;
}

/// A `CancelRequest` arrives on a new connection and must reach the backend serving the session it
/// cancels. The proxy does not track sessions (their `BackendKeyData` passes through the relay unparsed),
/// but the key is a random secret: deliver the request to every backend the listener can route to, and
/// only the one that issued the key acts on it. The protocol has no response to a `CancelRequest`.
void broadcastCancelRequest(const String & packet, const FrontendContext & ctx)
{
    size_t delivered = 0;
    for (const auto & backend : ctx.router.reachableBackends(ctx.listener))
    {
        /// A secure backend cannot be reached over this protocol (see `connectToBackend`). A backend that
        /// is marked down still receives the request: `alive` only gates new routing, and the sessions that
        /// were relayed to it before it was marked down may still be running there.
        if (backend->config().secure)
            continue;
        try
        {
            const UInt16 port = backendPortFor(ListenerProtocol::PostgreSQL, backend->config(), ctx.listener.port);
            FiberSocket socket = FiberSocket::connect(resolveAddress(backend->config().host, port), ctx.config.connect_timeout_ms);
            socket.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.handshake_timeout_ms);
            socket.sendAll(packet.data(), packet.size());
            socket.close();
            ++delivered;
        }
        catch (...)
        {
            /// Not a health signal (the request is sent to backends that may not serve this session):
            /// only log it. The other backends still receive the request.
            LOG_DEBUG(ctx.log, "Cannot deliver a PostgreSQL CancelRequest to backend {}: {}", backend->name(),
                getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        }
    }
    LOG_DEBUG(ctx.log, "Delivered a PostgreSQL CancelRequest to {} backends", delivered);
}

}

void handlePostgreSQL(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::PostgreSQL;
    attributes.peer_address = client.peerAddress().host().toString();

    /// When a rule needs the user or the database, the proxy must read them from the cleartext
    /// `StartupMessage`, so it declines a request to encrypt the connection: the client then either
    /// continues in cleartext or, if it requires encryption, disconnects. Routing such a connection by
    /// the default pool instead would silently ignore the rules.
    const bool needs_credentials = ctx.router.needsCredentials(ListenerProtocol::PostgreSQL);

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
                parseStartupParameters(params, attributes);
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

                /// No rule needs the user or the database: route by peer address or the default pool
                /// and forward the bytes verbatim, so the backend negotiates encryption end to end.
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
