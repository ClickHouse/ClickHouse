#include <Server/WebTerminalRequestHandler.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandler.h>
#include <Server/HTTPResponseHeaderWriter.h>
#include <Server/IServer.h>
#include <Server/ClientEmbedded/ClientEmbeddedRunner.h>
#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Access/Credentials.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Base64.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/SHA1Engine.h>

#include <base/errnoToString.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>


/// Embedded HTML page
constexpr unsigned char resource_webterminal_html[] =
{
#embed "../../programs/server/webterminal.html"
};


namespace DB
{

namespace
{

/// Compute the Sec-WebSocket-Accept value per RFC 6455
String computeWebSocketAccept(const String & key)
{
    const String magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    Poco::SHA1Engine sha1;
    sha1.update(key + magic);
    auto digest = sha1.digest();
    return base64Encode(String(reinterpret_cast<const char *>(digest.data()), digest.size()));
}

/// Validate that Sec-WebSocket-Key is a base64-encoded 16-byte nonce per RFC 6455.
/// `base64Decode` (via `Poco::Base64Decoder`) throws `DataFormatException` on
/// malformed input, so swallow exceptions here and treat any decode failure
/// as an invalid key. Otherwise a crafted header would escape as a `500`
/// instead of the deterministic `400` handshake rejection the caller expects.
bool isValidWebSocketKey(const String & key)
{
    if (key.empty() || key.size() > 128)
        return false;
    try
    {
        return base64Decode(key).size() == 16;
    }
    catch (...) /// Ok: malformed base64 is just an invalid key; the caller maps it to a 400 handshake rejection.
    {
        return false;
    }
}

/// Send all bytes to the socket, handling partial writes.
void sendAllBytes(Poco::Net::StreamSocket & socket, const char * data, size_t len)
{
    size_t total = 0;
    while (total < len)
    {
        int sent = socket.sendBytes(data + total, static_cast<int>(len - total));
        if (sent <= 0)
            throw Poco::IOException("Failed to send bytes to WebSocket");
        total += static_cast<size_t>(sent);
    }
}

/// Send a WebSocket frame. Server-to-client frames are not masked.
/// Header and payload are combined into a single buffer to avoid partial frame writes.
void sendWebSocketFrame(Poco::Net::StreamSocket & socket, uint8_t opcode, const char * data, size_t len)
{
    uint8_t header[10];
    size_t header_len = 2;

    header[0] = 0x80 | opcode; /// FIN + opcode

    if (len < 126)
    {
        header[1] = static_cast<uint8_t>(len);
    }
    else if (len < 65536)
    {
        header[1] = 126;
        header[2] = static_cast<uint8_t>((len >> 8) & 0xFF);
        header[3] = static_cast<uint8_t>(len & 0xFF);
        header_len = 4;
    }
    else
    {
        header[1] = 127;
        for (int i = 0; i < 8; ++i)
            header[2 + i] = static_cast<uint8_t>((len >> (56 - 8 * i)) & 0xFF);
        header_len = 10;
    }

    /// Combine header and payload into a single send to avoid partial frame writes
    String buf;
    buf.reserve(header_len + len);
    buf.append(reinterpret_cast<const char *>(header), header_len);
    if (len > 0)
        buf.append(data, len);
    sendAllBytes(socket, buf.data(), buf.size());
}

void sendWebSocketBinary(Poco::Net::StreamSocket & socket, const char * data, size_t len)
{
    sendWebSocketFrame(socket, 0x02, data, len);
}

void sendWebSocketClose(Poco::Net::StreamSocket & socket, uint16_t code, const String & reason)
{
    String payload;
    payload.push_back(static_cast<char>((code >> 8) & 0xFF));
    payload.push_back(static_cast<char>(code & 0xFF));
    payload.append(reason);
    sendWebSocketFrame(socket, 0x08, payload.data(), payload.size());
}

/// Read exactly n bytes from the socket.
/// If deadline_ns is non-zero, abort when the monotonic clock exceeds it.
bool readExact(Poco::Net::StreamSocket & socket, char * buf, size_t n, UInt64 deadline_ns = 0)
{
    size_t total = 0;
    while (total < n)
    {
        if (deadline_ns && clock_gettime_ns() > deadline_ns)
            return false;
        int received = socket.receiveBytes(buf + total, static_cast<int>(n - total));
        if (received <= 0)
            return false;
        total += static_cast<size_t>(received);
    }
    return true;
}

struct WebSocketFrame
{
    uint8_t opcode = 0;
    bool fin = false;
    String payload;
    bool valid = false;
    bool protocol_error = false;
    /// Set when the advertised frame payload length exceeds `MAX_FRAME_SIZE`.
    /// Distinct from `protocol_error` so callers can map it to the dedicated
    /// `1009` close code instead of `1002`.
    bool message_too_big = false;
};

/// Read a single WebSocket frame from the socket.
/// If deadline_ns is non-zero, abort when the monotonic clock exceeds it.
/// `max_payload_size` caps the advertised payload length and is set tighter
/// for pre-auth frames to prevent a single unauthenticated client from
/// forcing the server to allocate up to 16 MiB before any credentials have
/// been checked.
WebSocketFrame readWebSocketFrame(Poco::Net::StreamSocket & socket, UInt64 deadline_ns = 0, uint64_t max_payload_size = 16 * 1024 * 1024)
{
    WebSocketFrame frame;
    uint8_t header[2];

    if (!readExact(socket, reinterpret_cast<char *>(header), 2, deadline_ns))
        return frame;

    frame.fin = (header[0] & 0x80) != 0;
    frame.opcode = header[0] & 0x0F;

    /// RSV1/RSV2/RSV3 must be zero (no extensions negotiated)
    if (header[0] & 0x70)
    {
        frame.protocol_error = true;
        return frame;
    }

    /// Reject reserved opcodes per RFC 6455 Section 5.2
    if ((frame.opcode >= 0x03 && frame.opcode <= 0x07) || frame.opcode >= 0x0B)
    {
        frame.protocol_error = true;
        return frame;
    }

    bool masked = (header[1] & 0x80) != 0;
    uint64_t payload_len = header[1] & 0x7F;

    /// RFC 6455: client-to-server frames MUST be masked
    if (!masked)
    {
        frame.protocol_error = true;
        return frame;
    }

    /// Control frames (opcode >= 0x08) must have FIN set and payload <= 125
    if (frame.opcode >= 0x08)
    {
        if (!frame.fin || payload_len > 125)
        {
            frame.protocol_error = true;
            return frame;
        }
    }

    if (payload_len == 126)
    {
        uint8_t ext[2];
        if (!readExact(socket, reinterpret_cast<char *>(ext), 2, deadline_ns))
            return frame;
        payload_len = (static_cast<uint64_t>(ext[0]) << 8) | ext[1];
    }
    else if (payload_len == 127)
    {
        uint8_t ext[8];
        if (!readExact(socket, reinterpret_cast<char *>(ext), 8, deadline_ns))
            return frame;
        payload_len = 0;
        for (const auto & byte : ext)
            payload_len = (payload_len << 8) | byte;
    }

    /// Limit frame size to prevent DoS. Surface this as an explicit
    /// `message_too_big` state so the caller can close with `1009` rather
    /// than silently terminating the session (which would look like a
    /// normal `1000` close to the client).
    if (payload_len > max_payload_size)
    {
        frame.message_too_big = true;
        return frame;
    }

    uint8_t mask_key[4] = {};
    if (!readExact(socket, reinterpret_cast<char *>(mask_key), 4, deadline_ns))
        return frame;

    frame.payload.resize(payload_len);
    if (payload_len > 0)
    {
        if (!readExact(socket, frame.payload.data(), payload_len, deadline_ns))
            return frame;

        for (uint64_t i = 0; i < payload_len; ++i)
            frame.payload[i] ^= static_cast<char>(mask_key[i % 4]);
    }

    frame.valid = true;
    return frame;
}

/// Parse the JSON document and return its root object. Throws on malformed
/// input; the caller treats that as an invalid message and closes the socket.
/// We must parse the full structure (rather than searching for keys by
/// substring) because top-level fields like `type`/`user`/`password` could
/// otherwise be matched inside nested objects in attacker-controlled input.
Poco::JSON::Object::Ptr parseRootObject(const String & json)
{
    Poco::JSON::Parser parser;
    return parser.parse(json).extract<Poco::JSON::Object::Ptr>();
}

/// Read a top-level string field. Returns empty string if the key is missing.
/// Throws on type mismatch.
String getStringField(const Poco::JSON::Object::Ptr & object, const char * key)
{
    if (!object->has(key))
        return {};
    return object->getValue<String>(key);
}

/// Read a top-level integer field, clamped to [0, max_value]. Returns -1 if
/// the key is missing. Throws on type mismatch.
int getIntField(const Poco::JSON::Object::Ptr & object, const char * key, int max_value)
{
    if (!object->has(key))
        return -1;
    Int64 value = object->getValue<Int64>(key);
    if (value < 0)
        return -1;
    if (value > max_value)
        return max_value;
    return static_cast<int>(value);
}

/// Parse a control message like {"type":"resize","cols":80,"rows":24}.
bool parseResizeMessage(const String & json, int & cols, int & rows)
{
    auto object = parseRootObject(json);
    if (!object)
        return false;
    if (getStringField(object, "type") != "resize")
        return false;

    static constexpr int MAX_TERMINAL_DIMENSION = 500;
    cols = getIntField(object, "cols", MAX_TERMINAL_DIMENSION);
    rows = getIntField(object, "rows", MAX_TERMINAL_DIMENSION);
    return cols > 0 && rows > 0;
}

/// Parse an auth message like {"type":"auth","user":"default","password":"..."}.
bool parseAuthMessage(const String & json, String & user, String & password)
{
    auto object = parseRootObject(json);
    if (!object)
        return false;
    if (getStringField(object, "type") != "auth")
        return false;

    user = getStringField(object, "user");
    password = getStringField(object, "password");
    /// User is required, password can be empty
    return !user.empty();
}

}


void WebTerminalRequestHandler::serveHTML(HTTPServerRequest & request, HTTPServerResponse & response)
{
    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);

    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(reinterpret_cast<const char *>(resource_webterminal_html), std::size(resource_webterminal_html));
    wb.finalize();
}


void WebTerminalRequestHandler::handleWebSocket(HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto log = getLogger("WebTerminalHandler");

    /// Require GET method for WebSocket upgrade per RFC 6455
    if (request.getMethod() != HTTPRequest::HTTP_GET)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
        *response.send() << "WebSocket upgrade requires GET method.\n";
        return;
    }

    /// Validate WebSocket upgrade headers per RFC 6455
    String ws_key = request.get("Sec-WebSocket-Key", "");
    if (!isValidWebSocketKey(ws_key))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << "Invalid or missing Sec-WebSocket-Key.\n";
        return;
    }

    /// RFC 6455 fixed the WebSocket protocol at version 13; earlier drafts
    /// (00, 7, 8, etc.) are obsolete and not implemented here.
    String ws_version = request.get("Sec-WebSocket-Version", "");
    if (ws_version != "13")
    {
        response.set("Sec-WebSocket-Version", "13");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << "Unsupported WebSocket version.\n";
        return;
    }

    /// Validate Origin header to prevent cross-site WebSocket hijacking.
    /// Browsers always send the Origin header on WebSocket upgrades, so a
    /// missing `Origin` is either a non-browser client or a malicious
    /// upgrade. We refuse the request rather than allow it through: this is
    /// an interactive PTY, the cost of accepting a forged or unattributed
    /// origin is too high. Non-browser test clients must send a synthetic
    /// `Origin` matching the request host (see `tests/integration/test_webterminal`).
    ///
    /// By default, we enforce same-origin: the Origin must match the request in
    /// both scheme (`http`/`https`) and host/port. Comparing only host would
    /// allow a less-trusted page served over `http` on the same host to open a
    /// WebSocket against `https`, weakening CSWSH protection.
    ///
    /// Behind a TLS-terminating reverse proxy, `request.isSecure()` is `false`
    /// even when the browser sees `https`, so the strict check would reject
    /// legitimate same-origin connections. For such deployments, operators can
    /// set `webterminal_allowed_origins` in the server config to a
    /// comma-separated list of full origins (e.g.
    /// `https://example.com,https://app.example.com:8443`); when non-empty,
    /// this allowlist replaces the same-origin check.
    String origin = request.get("Origin", "");
    if (origin.empty())
    {
        LOG_WARNING(log, "WebSocket upgrade rejected: missing Origin header");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
        *response.send() << "Origin header is required.\n";
        return;
    }

    /// Strip trailing slash from Origin to normalize comparisons.
    String normalized_origin = origin;
    if (normalized_origin.back() == '/')
        normalized_origin.pop_back();

    String allowed_origins = server.config().getString("webterminal_allowed_origins", "");

    if (!allowed_origins.empty())
    {
        bool allowed = false;
        size_t pos = 0;
        while (pos <= allowed_origins.size())
        {
            size_t comma = allowed_origins.find(',', pos);
            size_t end_pos = (comma == String::npos) ? allowed_origins.size() : comma;
            /// Trim whitespace from this segment.
            size_t start = allowed_origins.find_first_not_of(" \t", pos);
            if (start != String::npos && start < end_pos)
            {
                size_t last = allowed_origins.find_last_not_of(" \t", end_pos - 1);
                String candidate = allowed_origins.substr(start, last - start + 1);
                if (!candidate.empty() && candidate.back() == '/')
                    candidate.pop_back();
                if (candidate == normalized_origin)
                {
                    allowed = true;
                    break;
                }
            }
            if (comma == String::npos)
                break;
            pos = comma + 1;
        }

        if (!allowed)
        {
            LOG_WARNING(log, "WebSocket Origin not in allowlist: origin={}", origin);
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
            *response.send() << "Origin not allowed.\n";
            return;
        }
    }
    else
    {
        String host = request.getHost();
        /// Split Origin into scheme and host (e.g. "https://example.com:8443" -> "https", "example.com:8443").
        size_t origin_scheme_end = normalized_origin.find("://");
        String origin_scheme;
        String origin_host;
        if (origin_scheme_end != String::npos)
        {
            origin_scheme = normalized_origin.substr(0, origin_scheme_end);
            origin_host = normalized_origin.substr(origin_scheme_end + 3);
        }
        else
        {
            origin_host = normalized_origin;
        }

        const String request_scheme = request.isSecure() ? "https" : "http";

        if (origin_host != host || origin_scheme != request_scheme)
        {
            LOG_WARNING(log, "WebSocket Origin mismatch: origin={}, host={}, scheme={}. "
                             "If running behind a TLS-terminating proxy, configure `webterminal_allowed_origins`.",
                        origin, host, request_scheme);
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
            *response.send() << "Origin not allowed.\n";
            return;
        }
    }

    /// Complete the WebSocket handshake first (authentication happens in-band
    /// via the first WebSocket message, to avoid leaking credentials in the URL).
    Poco::Net::StreamSocket & socket = response.getSocket();
    String accept_key = computeWebSocketAccept(ws_key);

    String handshake_str = "HTTP/1.1 101 Switching Protocols\r\n"
                           "Upgrade: websocket\r\n"
                           "Connection: Upgrade\r\n"
                           "Sec-WebSocket-Accept: " + accept_key + "\r\n"
                           "\r\n";
    sendAllBytes(socket, handshake_str.data(), handshake_str.size());

    /// Ensure the socket is always shut down after the 101 handshake,
    /// regardless of which code path we take (early auth failure, timeout, etc.).
    SCOPE_EXIT(
        try
        {
            socket.shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to shutdown WebSocket");
        }
    );

    /// Wait for the first WebSocket text message containing auth credentials:
    /// {"type":"auth","user":"...","password":"..."}
    /// Set a per-read timeout and a total deadline to prevent slow-trickle attacks
    /// where a malicious client sends one byte at a time to hold the connection.
    socket.setReceiveTimeout(Poco::Timespan(5, 0)); /// 5 seconds per individual read
    UInt64 auth_deadline = clock_gettime_ns() + 5'000'000'000ULL; /// 5 seconds total
    /// After the 101 handshake the stream is in WebSocket framing mode. If the
    /// client stalls mid-frame, `readWebSocketFrame` calls into `socket.receiveBytes`
    /// which throws `Poco::TimeoutException` on recv timeout. An uncaught exception
    /// would unwind without a close frame, so the browser would observe an abnormal
    /// close (`1006`) indistinguishable from a network drop. Catch read errors
    /// here and send an explicit policy close (`1008`) instead.
    ///
    /// Apply a tight pre-auth payload cap. The auth JSON is small
    /// (`{"type":"auth","user":...,"password":...}`), and the field-level
    /// limits below restrict `user` to 256 bytes and `password` to 1024 bytes,
    /// so 4 KiB is comfortably above any legitimate first frame. Without this
    /// cap, an unauthenticated client could force the server to allocate up
    /// to the general 16 MiB frame budget on every connection.
    static constexpr uint64_t MAX_AUTH_FRAME_SIZE = 4 * 1024;
    WebSocketFrame auth_frame;
    try
    {
        auth_frame = readWebSocketFrame(socket, auth_deadline, MAX_AUTH_FRAME_SIZE);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to read WebSocket auth frame");
        sendWebSocketClose(socket, 1008, "Auth read failed");
        return;
    }
    socket.setReceiveTimeout(Poco::Timespan(0)); /// Clear timeout for the main loop
    if (auth_frame.message_too_big)
    {
        sendWebSocketClose(socket, 1009, "Message too big");
        return;
    }
    if (auth_frame.protocol_error)
    {
        sendWebSocketClose(socket, 1002, "Protocol error");
        return;
    }
    if (!auth_frame.valid || auth_frame.opcode != 0x01)
    {
        sendWebSocketClose(socket, 1008, "Expected auth message");
        return;
    }

    /// Parse the auth message. `parseAuthMessage` (via `Poco::JSON::Parser` and
    /// `getStringField`) can throw on malformed JSON or type mismatch. After the
    /// 101 handshake we are in WebSocket framing mode, so an unhandled exception
    /// here would unwind without a close frame and the browser would see an
    /// abnormal close (1006) indistinguishable from a network drop. Catch
    /// parse errors and send a deterministic policy close (1008) instead.
    String auth_user;
    String auth_password;
    bool auth_parsed = false;
    try
    {
        auth_parsed = parseAuthMessage(auth_frame.payload, auth_user, auth_password);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to parse WebSocket auth message");
        sendWebSocketClose(socket, 1008, "Invalid auth message");
        return;
    }
    if (!auth_parsed)
    {
        sendWebSocketClose(socket, 1008, "Invalid auth message");
        return;
    }

    /// Enforce explicit limits on auth field sizes. The general frame-size cap
    /// of 16 MiB is far too permissive for credentials and would let a client
    /// force expensive auth work or oversized log payloads on failure.
    static constexpr size_t MAX_AUTH_USER_LENGTH = 256;
    static constexpr size_t MAX_AUTH_PASSWORD_LENGTH = 1024;
    if (auth_user.size() > MAX_AUTH_USER_LENGTH || auth_password.size() > MAX_AUTH_PASSWORD_LENGTH)
    {
        sendWebSocketClose(socket, 1008, "Auth field too long");
        return;
    }

    /// Authenticate the user. After the 101 handshake we are in WebSocket
    /// framing mode, so an unhandled exception here would unwind without a
    /// close frame and the browser would see an abnormal close (1006). Send
    /// an explicit policy-violation close (1008) instead so clients can
    /// distinguish login failure from network errors deterministically.
    /// Note: do not call makeSessionContext() here - it will be called by ClientEmbedded's constructor.
    auto session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP, request.isSecure());
    try
    {
        session->authenticate(BasicCredentials(auth_user, auth_password), request.clientAddress());
    }
    catch (...)
    {
        tryLogCurrentException(log, "WebSocket authentication failed");
        sendWebSocketClose(socket, 1008, "Authentication failed");
        return;
    }

    LOG_DEBUG(log, "WebSocket connection established for user {}", auth_user);

    /// Create PTY for the embedded client.
    /// 80x24 is the conventional default terminal geometry inherited from the
    /// VT100 / DEC VT family; see https://en.wikipedia.org/wiki/VT100. The
    /// browser sends a `resize` control message right after connecting, so this
    /// only matters until the first resize.
    static constexpr int DEFAULT_COLS = 80;
    static constexpr int DEFAULT_ROWS = 24;

    /// `xterm-256color` is the terminfo entry advertised to clickhouse-client
    /// inside the PTY. It is the most broadly-available entry that signals
    /// 256-color and modern xterm-style escape sequences. We do not request a
    /// truecolor entry like `xterm-direct`, because terminfo databases inside
    /// the container may not include it; clickhouse-client itself emits 24-bit
    /// (truecolor) ANSI escape sequences directly, so the chosen TERM only
    /// affects libraries (e.g. ncurses) that consult terminfo.
    auto pty_descriptors = std::make_unique<PtyClientDescriptorSet>("xterm-256color", DEFAULT_COLS, DEFAULT_ROWS, 0, 0);
    auto client_runner = std::make_unique<ClientEmbeddedRunner>(std::move(pty_descriptors), std::move(session));

    /// `ClientEmbedded::run` interprets each entry of this map as a
    /// `--key value` pair appended to the embedded `clickhouse-client`
    /// command line (see `ClientEmbedded.cpp`). The map must therefore stay
    /// empty here: the web terminal does not have a safe path for the
    /// user-facing browser to set arbitrary CLI flags on the embedded
    /// client, and reusing the SSH protocol's `SetEnv` carrier here would
    /// turn any future user-controlled key into a CLI-flag injection.
    NameToNameMap envs;
    client_runner->run(envs);

    auto server_descriptors = client_runner->getDescriptorsForServer();
    int pty_master_fd = server_descriptors.out;

    /// Set a send timeout to avoid hanging indefinitely on slow/non-reading clients
    socket.setSendTimeout(Poco::Timespan(30, 0)); /// 30 seconds

    /// Make PTY master non-blocking for polling. If we cannot put it into
    /// non-blocking mode, the read loop below would deadlock on the first
    /// `read`, so abort the session early instead.
    int flags = fcntl(pty_master_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(pty_master_fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        LOG_DEBUG(log, "Failed to set PTY master non-blocking: {}", errnoToString());
        sendWebSocketClose(socket, 1011, "Internal error");
        return;
    }

    /// Main I/O loop: bridge WebSocket <-> PTY
    bool running = true;
    /// Track whether a close frame has already been sent so we do not
    /// emit a second close (or further data) after the close handshake,
    /// which would violate RFC 6455.
    bool close_sent = false;
    char pty_buf[4096];

    auto send_close_once = [&](uint16_t code, const String & reason)
    {
        if (close_sent)
            return;
        close_sent = true;
        sendWebSocketClose(socket, code, reason);
    };

    /// State for WebSocket frame reassembly (RFC 6455 fragmentation)
    String fragment_buffer;
    uint8_t fragment_opcode = 0;
    bool in_fragmented_message = false;
    static constexpr size_t MAX_MESSAGE_SIZE = 16 * 1024 * 1024;

    /// Set a receive timeout to prevent a malicious client from stalling the
    /// handler thread by trickling bytes after poll() indicates readability.
    /// If a timeout fires mid-frame, the catch block below terminates the
    /// connection (rather than continuing), so there is no protocol desync.
    socket.setReceiveTimeout(Poco::Timespan(30, 0)); /// 30 seconds per socket read

    while (running && !server.isCancelled() && !client_runner->hasFinished())
    {
        struct pollfd fds[2];
        fds[0].fd = socket.impl()->sockfd();
        fds[0].events = POLLIN;
        fds[0].revents = 0;
        fds[1].fd = pty_master_fd;
        fds[1].events = POLLIN;
        fds[1].revents = 0;

        int poll_result = poll(fds, 2, 100); /// 100ms timeout
        if (poll_result < 0)
        {
            if (errno == EINTR)
                continue;
            break;
        }

        /// Data from PTY -> send to WebSocket. A failure inside `sendWebSocketBinary`
        /// (for example, the client closing the socket) propagates out of
        /// the loop; `SCOPE_EXIT` shuts down the socket.
        if (fds[1].revents & POLLIN)
        {
            ssize_t n = read(pty_master_fd, pty_buf, sizeof(pty_buf));
            if (n > 0)
            {
                sendWebSocketBinary(socket, pty_buf, static_cast<size_t>(n));
            }
            else if (n == 0)
            {
                running = false;
            }
            else if (errno != EINTR && errno != EAGAIN)
            {
                running = false;
            }
        }

        /// Data from WebSocket -> send to PTY
        if (fds[0].revents & POLLIN)
        {
            try
            {
                /// Enforce an absolute per-frame deadline. The per-read
                /// `setReceiveTimeout` above bounds a single `receiveBytes` call,
                /// but a client could keep trickling one byte at a time within
                /// the per-read timeout to hold the handler thread indefinitely.
                /// The absolute deadline closes that hole by capping the total
                /// time spent reading a single frame.
                UInt64 frame_deadline = clock_gettime_ns() + 30'000'000'000ULL; /// 30 seconds per frame
                WebSocketFrame frame = readWebSocketFrame(socket, frame_deadline);

                if (frame.message_too_big)
                {
                    send_close_once(1009, "Message too big");
                    running = false;
                    continue;
                }

                if (frame.protocol_error)
                {
                    send_close_once(1002, "Protocol error");
                    running = false;
                    continue;
                }

                if (!frame.valid)
                {
                    running = false;
                    continue;
                }

                /// Handle control frames (can appear between fragmented data frames)
                if (frame.opcode >= 0x08)
                {
                    switch (frame.opcode)
                    {
                        case 0x08: /// Close frame
                            send_close_once(1000, "Bye");
                            running = false;
                            break;
                        case 0x09: /// Ping
                            sendWebSocketFrame(socket, 0x0A, frame.payload.data(), frame.payload.size());
                            break;
                        default:
                            break;
                    }
                    continue;
                }

                /// Handle data frames with fragmentation support (RFC 6455 Section 5.4)
                if (frame.opcode != 0x00)
                {
                    /// A new data frame while a fragmented message is in progress is a protocol error
                    if (in_fragmented_message)
                    {
                        send_close_once(1002, "Protocol error: new message during fragmentation");
                        running = false;
                        continue;
                    }
                    /// First frame of a message (text or binary)
                    fragment_opcode = frame.opcode;
                    fragment_buffer = std::move(frame.payload);
                    in_fragmented_message = !frame.fin;
                }
                else
                {
                    /// Continuation frame without a preceding data frame is a protocol error
                    if (!in_fragmented_message)
                    {
                        send_close_once(1002, "Protocol error: unexpected continuation frame");
                        running = false;
                        continue;
                    }
                    if (fragment_buffer.size() + frame.payload.size() > MAX_MESSAGE_SIZE)
                    {
                        LOG_WARNING(log, "WebSocket message exceeded size limit");
                        send_close_once(1009, "Message too big");
                        running = false;
                        continue;
                    }
                    fragment_buffer.append(frame.payload);
                    if (frame.fin)
                        in_fragmented_message = false;
                }

                if (!frame.fin)
                    continue; /// More fragments to come

                /// Complete message assembled
                switch (fragment_opcode)
                {
                    case 0x01: /// Text message - control message
                    {
                        int cols = 0;
                        int rows = 0;
                        if (parseResizeMessage(fragment_buffer, cols, rows))
                            client_runner->changeWindowSize(cols, rows, 0, 0);
                        break;
                    }
                    case 0x02: /// Binary message - terminal input
                    {
                        const char * write_ptr = fragment_buffer.data();
                        size_t remaining = fragment_buffer.size();
                        while (remaining > 0)
                        {
                            ssize_t written = write(server_descriptors.in, write_ptr, remaining);
                            if (written < 0)
                            {
                                if (errno == EINTR)
                                    continue;
                                if (errno == EAGAIN || errno == EWOULDBLOCK)
                                {
                                    /// Wait for the PTY to become writable before retrying.
                                    /// On poll() error, abort instead of busy-spinning.
                                    struct pollfd pfd = {};
                                    pfd.fd = server_descriptors.in;
                                    pfd.events = POLLOUT;
                                    int poll_rc = poll(&pfd, 1, 100); /// Wait up to 100ms
                                    if (poll_rc < 0 && errno != EINTR)
                                    {
                                        running = false;
                                        break;
                                    }
                                    continue;
                                }
                                running = false;
                                break;
                            }
                            if (written == 0)
                            {
                                running = false;
                                break;
                            }
                            write_ptr += written;
                            remaining -= static_cast<size_t>(written);
                        }
                        break;
                    }
                    default:
                        break;
                }
                fragment_buffer.clear();
            }
            catch (const Poco::TimeoutException &)
            {
                /// Timeout while reading a frame means the client is stalling.
                /// We must close the connection because the frame parser may have
                /// consumed a partial frame, making the stream unrecoverable.
                LOG_DEBUG(log, "WebSocket frame read timed out, closing connection");
                running = false;
            }
            catch (...)
            {
                LOG_DEBUG(log, "WebSocket read error: {}", getCurrentExceptionMessage(false));
                running = false;
            }
        }

        /// Check for socket errors
        if (fds[0].revents & (POLLERR | POLLHUP))
            running = false;
    }

    /// Drain remaining PTY output and send the final close frame only when
    /// no close has been emitted yet. RFC 6455 forbids data frames or a second
    /// close after the close handshake has started.
    if (!close_sent)
    {
        for (;;)
        {
            struct pollfd pfd = {};
            pfd.fd = pty_master_fd;
            pfd.events = POLLIN;
            if (poll(&pfd, 1, 0) <= 0 || !(pfd.revents & POLLIN))
                break;
            ssize_t n = read(pty_master_fd, pty_buf, sizeof(pty_buf));
            if (n <= 0)
                break;
            try
            {
                sendWebSocketBinary(socket, pty_buf, static_cast<size_t>(n));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to drain PTY output to WebSocket");
                break;
            }
        }

        send_close_once(1000, "Session ended");
    }

    /// socket.shutdown() is handled by SCOPE_EXIT above

    LOG_INFO(log, "WebSocket connection closed");
}


void WebTerminalRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    if (!server.config().getBool("allow_experimental_webterminal", false))
    {
        response.setContentType("text/plain; charset=UTF-8");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
        *response.send() << "Web terminal is disabled. See the `allow_experimental_webterminal` server configuration.\n";
        return;
    }

    /// Check if this is a WebSocket upgrade request.
    /// Per RFC 7230 the `Connection` header is a comma-separated list of
    /// tokens. A bare substring match for `upgrade` would also accept
    /// unrelated values that happen to contain those letters (e.g. a future
    /// `Connection: keep-alive, upgrade-something`), so split the header on
    /// commas, trim whitespace, and look for the exact token `upgrade`.
    String connection = request.get("Connection", "");
    std::transform(connection.begin(), connection.end(), connection.begin(), ::tolower);

    String upgrade = request.get("Upgrade", "");
    std::transform(upgrade.begin(), upgrade.end(), upgrade.begin(), ::tolower);

    bool has_upgrade_token = false;
    size_t pos = 0;
    while (pos <= connection.size() && !has_upgrade_token)
    {
        size_t comma = connection.find(',', pos);
        size_t end_pos = (comma == String::npos) ? connection.size() : comma;
        size_t start = connection.find_first_not_of(" \t", pos);
        if (start != String::npos && start < end_pos)
        {
            size_t last = connection.find_last_not_of(" \t", end_pos - 1);
            if (connection.substr(start, last - start + 1) == "upgrade")
                has_upgrade_token = true;
        }
        if (comma == String::npos)
            break;
        pos = comma + 1;
    }

    if (has_upgrade_token && upgrade == "websocket")
    {
        handleWebSocket(request, response);
    }
    else
    {
        /// Only allow GET/HEAD for the HTML page
        if (request.getMethod() != HTTPRequest::HTTP_GET && request.getMethod() != HTTPRequest::HTTP_HEAD)
        {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
            *response.send() << "Method not allowed.\n";
            return;
        }
        serveHTML(request, response);
    }
}

}
