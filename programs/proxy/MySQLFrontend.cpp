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
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <base/scope_guard.h>

#include <atomic>
#include <random>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}
}

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
    [[nodiscard]] size_t getPayloadSize() const override { return payload.size(); }
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

/// The connection ID of the greeting. The greeting is sent before the client names the user that selects the backend,
/// so it cannot carry the ID of the backend's session, and the command phase is not mediated to translate it.
/// The IDs are at least unique among the sessions of the proxy and are taken from the upper half of the range,
/// while a server counts its connections from zero: a client that kills "its" connection by this ID (`KILL QUERY <id>`,
/// `COM_PROCESS_KILL`) names a session that does not exist on the backend instead of the session of someone else.
uint32_t nextConnectionID()
{
    static std::atomic<uint32_t> counter {0};
    return 0x80000000U | counter.fetch_add(1, std::memory_order_relaxed);
}

constexpr size_t packet_header_size = 4;
constexpr size_t max_payload_size = 64 * 1024;
/// The payload of an `SSLRequest` has a fixed size, and a `HandshakeResponse` is always longer (it continues with the user name).
constexpr size_t ssl_request_payload_size = 32;

size_t payloadSize(const String & header, const char * description)
{
    const size_t payload_size = static_cast<UInt8>(header[0])
        | (static_cast<size_t>(static_cast<UInt8>(header[1])) << 8)
        | (static_cast<size_t>(static_cast<UInt8>(header[2])) << 16);
    if (payload_size > max_payload_size)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The MySQL {} declares a payload of {} bytes, while it must be at most {} bytes",
            description, payload_size, max_payload_size);
    return payload_size;
}

/// Reads the first packet of the client, with its header, straight from the socket and not a byte more.
/// The packet may be an `SSLRequest`, which the client follows with the TLS handshake without waiting for an answer:
/// a buffered read could take the beginning of the handshake away from the TLS layer.
String receiveFirstPacket(FiberSocket & client, const char * description)
{
    const auto read_exactly = [&](char * to, size_t size)
    {
        size_t pos = 0;
        while (pos < size)
        {
            const int received = client.receive(to + pos, static_cast<int>(size - pos));
            if (received <= 0)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "The client closed the connection in the middle of the MySQL {}", description);
            pos += received;
        }
    };

    String data(packet_header_size, '\0');
    read_exactly(data.data(), packet_header_size);
    const size_t payload_size = payloadSize(data, description);
    data.resize(packet_header_size + payload_size);
    read_exactly(data.data() + packet_header_size, payload_size);
    return data;
}

/// Reads one packet of the authentication phase. Neither peer is trusted at this point: the client is not authenticated,
/// and the backend may be faulty or chosen by the client (a dynamic backend). The packet readers of `PacketEndpoint`
/// join continuation packets and grow the strings of a packet without a total bound, so the payload size is checked
/// before the payload is read, as in `MySQLHandler::finishHandshake`. The bound also excludes continuation packets,
/// which only follow a payload of `0xffffff` bytes.
void receiveBoundedPacket(ReadBuffer & in, uint8_t & sequence_id, IMySQLReadPacket & packet, const char * description)
{
    String data(packet_header_size, '\0');
    in.readStrict(data.data(), packet_header_size);
    const size_t payload_size = payloadSize(data, description);
    data.resize(packet_header_size + payload_size);
    in.readStrict(data.data() + packet_header_size, payload_size);

    ReadBufferFromString data_in(data);
    packet.readPayload(data_in, sequence_id);
}

/// Terminate the MySQL handshake to learn the user and the database, route on them, then re-originate
/// the connection to the chosen backend. The client authenticated against the proxy's throwaway
/// scramble, so the proxy rebinds authentication to the backend's scramble with an auth-switch: the
/// client recomputes its response against the backend's scramble, which the proxy forwards. No
/// password is ever known to the proxy.
void terminateAndRoute(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    /// The capabilities of the session, as negotiated with the client and then with the backend.
    const uint32_t session_capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;

    /// TLS is negotiated in-band: the client answers the greeting with an `SSLRequest` and switches to TLS
    /// before it sends the `HandshakeResponse` with the user and the database. The proxy terminates it,
    /// so it offers TLS when it has a server certificate. The backend leg stays in plaintext.
#if USE_SSL
    const bool offer_tls = !ctx.server_tls_context.isNull();
#else
    const bool offer_tls = false;
#endif

    uint8_t client_seq = 0;
    {
        AutoCanceledWriteBuffer<WriteBufferFromPocoSocket> greeting_out(client.raw());
        PacketEndpoint greeting_endpoint(greeting_out, client_seq);
        /// The server version must begin with a MySQL-style numeric version; clients parse it and reject
        /// a greeting whose version does not start with digits.
        Handshake greeting(session_capabilities | (offer_tls ? CLIENT_SSL : 0), nextConnectionID(), "8.0.0-ClickHouse-proxy",
            "mysql_native_password", makeScramble(), /*charset=*/ 33);
        greeting_endpoint.sendPacket(greeting, true);
        greeting_out.finalize();
    }

    const String first_packet = receiveFirstPacket(client, "handshake response of the client");
    const bool tls_requested = first_packet.size() == packet_header_size + ssl_request_payload_size;
    if (tls_requested)
    {
        if (!offer_tls || static_cast<UInt8>(first_packet[3]) != client_seq)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected MySQL `SSLRequest` of the client");
        ++client_seq;
#if USE_SSL
        client.startServerTLS(ctx.server_tls_context);
        client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);
#endif
    }

    /// Created only now: the buffers read and write through the TLS session, if the client asked for one.
    ReadBufferFromPocoSocket client_in(client.raw());
    AutoCanceledWriteBuffer<WriteBufferFromPocoSocket> client_out(client.raw());
    PacketEndpoint client_ep(client_in, client_out, client_seq);

    HandshakeResponse response;
    if (tls_requested)
    {
        receiveBoundedPacket(client_in, client_seq, response, "handshake response of the client");
    }
    else
    {
        ReadBufferFromString first_packet_in(first_packet);
        response.readPayload(first_packet_in, client_seq);
    }

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

    /// The backend leg is in the handshake until it accepts the login; `runRelay` sets the steady-state timeouts.
    backend_socket.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);
    ReadBufferFromPocoSocket backend_in(backend_socket.raw());
    AutoCanceledWriteBuffer<WriteBufferFromPocoSocket> backend_out(backend_socket.raw());
    uint8_t backend_seq = 0;
    PacketEndpoint backend_ep(backend_in, backend_out, backend_seq);

    /// Read the backend greeting to learn its scramble.
    Handshake backend_greeting;
    receiveBoundedPacket(backend_in, backend_seq, backend_greeting, "greeting of the backend");

    /// Rebind: ask the client to re-authenticate against the backend's scramble. The scramble is
    /// sent NUL-terminated (20 bytes + NUL), as clients expect in an auth-switch request.
    AuthSwitchRequest auth_switch("mysql_native_password", backend_greeting.auth_plugin_data + '\0');
    client_ep.sendPacket(auth_switch, true);
    AuthSwitchResponse auth_switch_response;
    receiveBoundedPacket(client_in, client_seq, auth_switch_response, "auth-switch response of the client");

    /// Log in to the backend as the user, with the response now bound to the backend's scramble.
    /// The command phase is spliced byte for byte after authentication, so capability bits that
    /// change its framing (e.g. CLIENT_DEPRECATE_EOF result set endings) must be negotiated
    /// identically on both legs: forward what the client negotiated with the proxy, intersected
    /// with what the backend advertises. The bits governing the login itself are always set,
    /// because this packet is composed by the proxy, not spliced.
    uint32_t backend_request_capabilities
        = response.capability_flags & session_capabilities & backend_greeting.capability_flags;
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
    receiveBoundedPacket(backend_in, backend_seq, auth_result, "authentication result of the backend");
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

/// MySQL is server-speaks-first and negotiates TLS in-band. When a routing rule or a first-seen hook needs
/// the user or the database the proxy terminates the handshake (and TLS, if the client asks for it) to read them;
/// otherwise it forwards transparently, with whatever TLS the client and the backend negotiate, and
/// routes by peer address or the default pool.
void handleMySQL(FiberSocket & client, const FrontendContext & ctx)
{
    /// The first-seen hooks need the user and the database too.
    if (ctx.router.needsCredentials(ListenerProtocol::MySQL) || ctx.router.hasFirstSeenHooks())
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
