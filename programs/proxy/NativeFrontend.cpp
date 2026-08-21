#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>


namespace DB::Proxy
{

void handleNative(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::Native;
    attributes.peer_address = client.peerAddress().host().toString();

    RecordingReader reader(client);

    /// The proxy only needs the user and the database to route, so it parses the leading Hello
    /// packet and replays it verbatim to the backend. Routing by query type would require the proxy
    /// to answer the Hello itself and speak the client side of the protocol to the backend; that is
    /// out of scope here, so query-type rules do not apply to the native protocol.
    try
    {
        const UInt64 packet_type = reader.readVarUInt();
        if (packet_type != 0)     /// Protocol::Client::Hello
        {
            LOG_WARNING(ctx.log, "First native packet is not a Hello (type {}); closing", packet_type);
            return;
        }

        reader.readVarString();   /// client name
        reader.readVarUInt();     /// version major
        reader.readVarUInt();     /// version minor
        reader.readVarUInt();     /// protocol revision
        attributes.database = reader.readVarString();
        attributes.user = reader.readVarString();
        reader.readVarString();   /// password (cleartext on a plaintext port; ignored by the proxy)
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot parse native Hello: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
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

    LOG_DEBUG(ctx.log, "Routing native connection (user='{}', database='{}') to backend {}",
        attributes.user, attributes.database, backend.name());

    runRelay(client, backend_socket, &backend, reader.received(), ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

#endif
