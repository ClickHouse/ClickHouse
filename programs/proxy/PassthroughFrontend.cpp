#include <Frontend.h>

#if USE_SILK

#include <Relay.h>
#include <TLSSupport.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/String.h>

#include <base/scope_guard.h>


namespace DB::Proxy
{

void handleImmediatePassthrough(FiberSocket & client, const FrontendContext & ctx, RouteAttributes attributes)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);
    attributes.peer_address = client.peerAddress().host().toString();

    RouteResult route = routeConnection(ctx, attributes);
    if (!route.backend)
        return;

    Backend & backend = *route.backend;
    backend.onConnectionStart();
    SCOPE_EXIT({ backend.onConnectionEnd(); });

    FiberSocket backend_socket;
    try
    {
        backend_socket = connectToBackend(ctx, backend, backend.config().secure, attributes.host);
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot connect to backend {}: {}", backend.name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return;
    }

    LOG_DEBUG(ctx.log, "Routing {} connection to backend {}", toString(attributes.protocol), backend.name());

    runRelay(client, backend_socket, &backend, /*initial_to_backend=*/ "", ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

void handlePassthrough(FiberSocket & client, const FrontendContext & ctx)
{
    /// Opaque TCP forwarding: choose the backend up front and splice the connection.
    if (ctx.listener.protocol == ListenerProtocol::Stream)
    {
        RouteAttributes attributes;
        attributes.protocol = ListenerProtocol::Stream;
        handleImmediatePassthrough(client, ctx, attributes);
        return;
    }

    /// Transparent TLS routing: read the leading ClientHello, extract the SNI, and forward the encrypted
    /// stream to the backend chosen by hostname. The proxy never decrypts anything.
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::TLS;
    attributes.peer_address = client.peerAddress().host().toString();

    RecordingReader reader(client);
    try
    {
        if (auto sni = peekTLSClientHelloSNI(reader))
            attributes.host = Poco::toLower(*sni);   /// DNS hostnames are case-insensitive.
        else
            LOG_DEBUG(ctx.log, "No SNI in the TLS ClientHello; routing by peer address or the default pool");
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot parse the TLS ClientHello: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
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
        /// Forward the raw encrypted bytes; never wrap the backend leg in the proxy's own TLS.
        backend_socket = connectToBackend(ctx, backend, /*encrypt=*/ false, attributes.host);
    }
    catch (...)
    {
        LOG_WARNING(ctx.log, "Cannot connect to backend {}: {}", backend.name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return;
    }

    LOG_DEBUG(ctx.log, "Routing TLS connection (SNI='{}') to backend {}", attributes.host, backend.name());

    runRelay(client, backend_socket, &backend, reader.received(), ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

#endif
