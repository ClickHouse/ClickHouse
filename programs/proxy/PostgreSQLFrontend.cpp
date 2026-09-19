#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>


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

}

void handlePostgreSQL(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::PostgreSQL;
    attributes.peer_address = client.peerAddress().host().toString();

    RecordingReader reader(client);
    try
    {
        const UInt32 length = reader.readBE<UInt32>();
        const UInt32 code = reader.readBE<UInt32>();

        if (code == PROTOCOL_VERSION_3)
        {
            /// A cleartext StartupMessage: read its parameters to route by user and database.
            if (length < 8 || length > 1024 * 1024)
            {
                LOG_WARNING(ctx.log, "Invalid PostgreSQL StartupMessage length {}", length);
                return;
            }
            const String params = reader.readFixed(length - 8);
            parseStartupParameters(params, attributes);
        }
        else if (code == SSL_REQUEST || code == GSSENC_REQUEST || code == CANCEL_REQUEST)
        {
            /// The client wants to negotiate encryption (or is cancelling a query) before sending any
            /// credentials. The proxy cannot see the user or the database, so it routes by peer address
            /// or the default pool and forwards the bytes verbatim: the backend negotiates end to end.
            LOG_DEBUG(ctx.log, "PostgreSQL connection begins with a non-startup request ({}); "
                "routing by peer address or the default pool", code);
        }
        else
        {
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

    runRelay(client, backend_socket, &backend, reader.received(), ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

#endif
