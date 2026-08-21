#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <Core/MySQL/PacketEndpoint.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <base/scope_guard.h>

#include <random>


namespace DB::Proxy
{

using namespace MySQLProtocol;
using namespace MySQLProtocol::ConnectionPhase;
using namespace MySQLProtocol::Generic;

namespace
{

/// A MySQL packet whose payload is relayed verbatim between the two legs (used for the auth result).
struct RawPacket : public IMySQLWritePacket, public IMySQLReadPacket
{
    String payload;
    size_t getPayloadSize() const override { return payload.size(); }
    void writePayloadImpl(WriteBuffer & buffer) const override { buffer.write(payload.data(), payload.size()); }
    void readPayloadImpl(ReadBuffer & buffer) override { readStringUntilEOF(payload, buffer); }
};

String makeScramble()
{
    /// 20 bytes of scramble plus a trailing NUL. The NUL matters: clients read the second part of
    /// the auth-plugin-data as max(13, total - 8) bytes, so the total must be 21 (8 + 13), otherwise
    /// the client reads into the auth-plugin name and rejects the greeting.
    String scramble;
    scramble.resize(21, '\0');
    std::uniform_int_distribution<int> dist(1, 253);   /// Avoid NUL inside the scramble itself.
    for (size_t i = 0; i < 20; ++i)
        scramble[i] = static_cast<char>(dist(thread_local_rng));
    return scramble;
}

/// Bytes already buffered in a socket read buffer (belonging to the command phase), if any.
String drainBuffered(ReadBuffer & in)
{
    String rest;
    if (in.available())
    {
        rest.assign(in.position(), in.buffer().end());
        in.position() = in.buffer().end();
    }
    return rest;
}

/// Terminate the MySQL handshake to learn the user and the database, route on them, then re-originate
/// the connection to the chosen backend. The client authenticated against the proxy's throwaway
/// scramble, so the proxy rebinds authentication to the backend's scramble with an auth-switch: the
/// client recomputes its response against the backend's scramble, which the proxy forwards. No
/// password is ever known to the proxy.
void terminateAndRoute(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    ReadBufferFromPocoSocket client_in(client.raw());
    AutoCanceledWriteBuffer<WriteBufferFromPocoSocket> client_out(client.raw());
    uint8_t client_seq = 0;
    PacketEndpoint client_ep(client_in, client_out, client_seq);

    /// Greet the client. TLS is deliberately not offered so the handshake stays readable.
    const uint32_t server_capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;
    const String proxy_scramble = makeScramble();
    /// The server version must begin with a MySQL-style numeric version; clients parse it and reject
    /// a greeting whose version does not start with digits.
    Handshake greeting(server_capabilities, /*connection_id=*/ 1, "8.0.0-ClickHouse-proxy",
        "mysql_native_password", proxy_scramble, /*charset=*/ 33);
    client_ep.sendPacket(greeting, true);

    HandshakeResponse response;
    client_ep.receivePacket(response);

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::MySQL;
    attributes.peer_address = client.peerAddress().host().toString();
    attributes.user = response.username;
    attributes.database = response.database;

    RouteResult route = routeConnection(ctx, attributes);
    if (!route.backend)
    {
        ERRPacket err(1045, "28000", "ClickHouse proxy: " + (route.failure_reason.empty() ? "no backend" : route.failure_reason));
        client_ep.sendPacket(err, true);
        return;
    }

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
        ERRPacket err(1045, "28000", "ClickHouse proxy: cannot connect to backend");
        client_ep.sendPacket(err, true);
        return;
    }

    backend_socket.setTimeouts(ctx.config.send_timeout_ms, ctx.config.send_timeout_ms);
    ReadBufferFromPocoSocket backend_in(backend_socket.raw());
    AutoCanceledWriteBuffer<WriteBufferFromPocoSocket> backend_out(backend_socket.raw());
    uint8_t backend_seq = 0;
    PacketEndpoint backend_ep(backend_in, backend_out, backend_seq);

    /// Read the backend greeting to learn its scramble.
    Handshake backend_greeting;
    backend_ep.receivePacket(backend_greeting);

    /// Rebind: ask the client to re-authenticate against the backend's scramble. The scramble is
    /// sent NUL-terminated (20 bytes + NUL), as clients expect in an auth-switch request.
    AuthSwitchRequest auth_switch("mysql_native_password", backend_greeting.auth_plugin_data + '\0');
    client_ep.sendPacket(auth_switch, true);
    AuthSwitchResponse auth_switch_response;
    client_ep.receivePacket(auth_switch_response);

    /// Log in to the backend as the user, with the response now bound to the backend's scramble.
    /// The command phase is spliced byte for byte after authentication, so capability bits that
    /// change its framing (e.g. CLIENT_DEPRECATE_EOF result set endings) must be negotiated
    /// identically on both legs: forward what the client negotiated with the proxy, intersected
    /// with what the backend advertises. The bits governing the login itself are always set,
    /// because this packet is composed by the proxy, not spliced.
    uint32_t backend_request_capabilities
        = response.capability_flags & server_capabilities & backend_greeting.capability_flags;
    backend_request_capabilities |= CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    if (!response.database.empty())
        backend_request_capabilities |= CLIENT_CONNECT_WITH_DB;
    else
        backend_request_capabilities &= ~static_cast<uint32_t>(CLIENT_CONNECT_WITH_DB);
    HandshakeResponse backend_response(backend_request_capabilities, response.max_packet_size, response.character_set,
        response.username, response.database, auth_switch_response.value, "mysql_native_password");
    backend_ep.sendPacket(backend_response, true);

    /// Relay the backend's authentication result back to the client (sequence numbers are renumbered
    /// per leg by the two endpoints). On success both legs enter the command phase with a reset
    /// sequence, so the rest of the connection can be spliced byte for byte.
    RawPacket auth_result;
    backend_ep.receivePacket(auth_result);
    const UInt8 result_type = auth_result.payload.empty() ? 0xFF : static_cast<UInt8>(auth_result.payload[0]);
    client_ep.sendPacket(auth_result, true);

    if (result_type == 0xFF)     /// ERR: authentication failed.
        return;
    if (result_type != 0x00)     /// Anything other than OK (e.g. an auth-method switch we do not mediate).
    {
        LOG_WARNING(ctx.log, "Backend {} requested an unsupported MySQL authentication exchange (0x{:02x})",
            backend.name(), static_cast<int>(result_type));
        return;
    }

    LOG_DEBUG(ctx.log, "Routing MySQL connection (user='{}', database='{}') to backend {}",
        attributes.user, attributes.database, backend.name());

    /// The auth phase is over; flush and finalize the packet write buffers before handing the raw
    /// sockets to the splicer.
    client_out.finalize();
    backend_out.finalize();

    /// Hand any already-buffered command bytes across, then splice the rest.
    const String to_client = drainBuffered(backend_in);
    if (!to_client.empty())
        client.sendAll(to_client.data(), to_client.size());
    const String to_backend = drainBuffered(client_in);

    runRelay(client, backend_socket, &backend, to_backend, ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

/// MySQL is server-speaks-first and negotiates TLS in-band. When a routing rule needs the user or the
/// database the proxy terminates the handshake to read them; otherwise it forwards transparently and
/// routes by peer address or the default pool.
void handleMySQL(FiberSocket & client, const FrontendContext & ctx)
{
    if (ctx.router.needsCredentials(ListenerProtocol::MySQL))
    {
        try
        {
            terminateAndRoute(client, ctx);
        }
        catch (...)
        {
            LOG_DEBUG(ctx.log, "MySQL connection failed: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        }
        return;
    }

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::MySQL;
    handleImmediatePassthrough(client, ctx, attributes);
}

}

#endif
