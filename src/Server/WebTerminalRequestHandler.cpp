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
#include <Poco/Net/StreamSocket.h>
#include <Poco/SHA1Engine.h>

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

/// Validate that Sec-WebSocket-Key is a base64-encoded 16-byte nonce per RFC 6455
bool isValidWebSocketKey(const String & key)
{
    if (key.empty() || key.size() > 128)
        return false;
    String decoded = base64Decode(key);
    return decoded.size() == 16;
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
bool readExact(Poco::Net::StreamSocket & socket, char * buf, size_t n)
{
    size_t total = 0;
    while (total < n)
    {
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
};

/// Read a single WebSocket frame from the socket.
WebSocketFrame readWebSocketFrame(Poco::Net::StreamSocket & socket)
{
    WebSocketFrame frame;
    uint8_t header[2];

    if (!readExact(socket, reinterpret_cast<char *>(header), 2))
        return frame;

    frame.fin = (header[0] & 0x80) != 0;
    frame.opcode = header[0] & 0x0F;

    /// RSV1/RSV2/RSV3 must be zero (no extensions negotiated)
    if (header[0] & 0x70)
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
        if (!readExact(socket, reinterpret_cast<char *>(ext), 2))
            return frame;
        payload_len = (static_cast<uint64_t>(ext[0]) << 8) | ext[1];
    }
    else if (payload_len == 127)
    {
        uint8_t ext[8];
        if (!readExact(socket, reinterpret_cast<char *>(ext), 8))
            return frame;
        payload_len = 0;
        for (const auto & byte : ext)
            payload_len = (payload_len << 8) | byte;
    }

    /// Limit frame size to prevent DoS
    static constexpr uint64_t MAX_FRAME_SIZE = 16 * 1024 * 1024;
    if (payload_len > MAX_FRAME_SIZE)
        return frame;

    uint8_t mask_key[4] = {};
    if (!readExact(socket, reinterpret_cast<char *>(mask_key), 4))
        return frame;

    frame.payload.resize(payload_len);
    if (payload_len > 0)
    {
        if (!readExact(socket, frame.payload.data(), payload_len))
            return frame;

        for (uint64_t i = 0; i < payload_len; ++i)
            frame.payload[i] ^= static_cast<char>(mask_key[i % 4]);
    }

    frame.valid = true;
    return frame;
}

/// Extract a JSON string value for a given key from a simple flat JSON object.
/// Returns empty string if not found.
String extractJSONStringValue(const String & json, const char * key)
{
    auto pos = json.find(key);
    if (pos == String::npos)
        return {};
    pos += strlen(key);
    pos = json.find(':', pos);
    if (pos == String::npos)
        return {};
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
        ++pos;
    if (pos >= json.size() || json[pos] != '"')
        return {};
    ++pos;
    String result;
    while (pos < json.size() && json[pos] != '"')
    {
        if (json[pos] == '\\' && pos + 1 < json.size())
        {
            ++pos;
            switch (json[pos])
            {
                case '"': result += '"'; break;
                case '\\': result += '\\'; break;
                case '/': result += '/'; break;
                case 'n': result += '\n'; break;
                case 't': result += '\t'; break;
                case 'r': result += '\r'; break;
                default: result += json[pos]; break;
            }
        }
        else
        {
            result += json[pos];
        }
        ++pos;
    }
    return result;
}

/// Extract a JSON integer value for a given key, clamped to [0, max_value].
/// Returns -1 if not found.
int extractJSONIntValue(const String & json, const char * key, int max_value)
{
    auto pos = json.find(key);
    if (pos == String::npos)
        return -1;
    pos += strlen(key);
    pos = json.find(':', pos);
    if (pos == String::npos)
        return -1;
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
        ++pos;
    unsigned value = 0;
    while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9')
    {
        value = value * 10 + static_cast<unsigned>(json[pos] - '0');
        if (value > static_cast<unsigned>(max_value))
            return max_value;
        ++pos;
    }
    return static_cast<int>(value);
}

/// Parse a simple JSON message like {"type":"resize","cols":80,"rows":24}
/// This is a minimal parser sufficient for our control messages.
bool parseResizeMessage(const String & json, int & cols, int & rows)
{
    if (json.find("\"resize\"") == String::npos)
        return false;

    static constexpr int MAX_TERMINAL_DIMENSION = 500;

    cols = extractJSONIntValue(json, "\"cols\"", MAX_TERMINAL_DIMENSION);
    rows = extractJSONIntValue(json, "\"rows\"", MAX_TERMINAL_DIMENSION);
    return cols > 0 && rows > 0;
}

/// Parse an auth message like {"type":"auth","user":"default","password":"..."}
bool parseAuthMessage(const String & json, String & user, String & password)
{
    if (json.find("\"auth\"") == String::npos)
        return false;

    user = extractJSONStringValue(json, "\"user\"");
    password = extractJSONStringValue(json, "\"password\"");
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

    String ws_version = request.get("Sec-WebSocket-Version", "");
    if (ws_version != "13")
    {
        response.set("Sec-WebSocket-Version", "13");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << "Unsupported WebSocket version.\n";
        return;
    }

    /// Validate Origin header to prevent cross-site WebSocket hijacking.
    /// Browsers always send the Origin header on WebSocket upgrades.
    /// We enforce same-origin: the Origin must match the Host header.
    String origin = request.get("Origin", "");
    if (!origin.empty())
    {
        String host = request.getHost();
        /// Extract host from Origin URL (e.g. "https://example.com:8443" -> "example.com:8443")
        size_t origin_host_start = origin.find("://");
        String origin_host = (origin_host_start != String::npos) ? origin.substr(origin_host_start + 3) : origin;
        /// Remove trailing slash if present
        if (!origin_host.empty() && origin_host.back() == '/')
            origin_host.pop_back();

        if (origin_host != host)
        {
            LOG_WARNING(log, "WebSocket Origin mismatch: origin={}, host={}", origin, host);
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

    /// Wait for the first WebSocket text message containing auth credentials:
    /// {"type":"auth","user":"...","password":"..."}
    WebSocketFrame auth_frame = readWebSocketFrame(socket);
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

    String auth_user;
    String auth_password;
    if (!parseAuthMessage(auth_frame.payload, auth_user, auth_password))
    {
        sendWebSocketClose(socket, 1008, "Invalid auth message");
        return;
    }

    /// Authenticate the user
    auto session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP, request.isSecure());
    try
    {
        session->authenticate(BasicCredentials(auth_user, auth_password), request.clientAddress());
        session->makeSessionContext();
    }
    catch (...)
    {
        LOG_WARNING(log, "WebSocket authentication failed for user {}: {}", auth_user, getCurrentExceptionMessage(false));
        sendWebSocketClose(socket, 1008, "Authentication failed");
        return;
    }

    LOG_INFO(log, "WebSocket connection established for user {}", auth_user);

    /// Create PTY for the embedded client
    static constexpr int DEFAULT_COLS = 80;
    static constexpr int DEFAULT_ROWS = 24;

    auto pty_descriptors = std::make_unique<PtyClientDescriptorSet>("xterm-256color", DEFAULT_COLS, DEFAULT_ROWS, 0, 0);
    auto client_runner = std::make_unique<ClientEmbeddedRunner>(std::move(pty_descriptors), std::move(session));

    NameToNameMap envs;
    client_runner->run(envs);

    auto server_descriptors = client_runner->getDescriptorsForServer();
    int pty_master_fd = server_descriptors.out;

    /// Set a send timeout to avoid hanging indefinitely on slow/non-reading clients
    socket.setSendTimeout(Poco::Timespan(30, 0)); /// 30 seconds

    /// Make PTY master non-blocking for polling
    int flags = fcntl(pty_master_fd, F_GETFL, 0);
    fcntl(pty_master_fd, F_SETFL, flags | O_NONBLOCK);

    /// Main I/O loop: bridge WebSocket <-> PTY
    bool running = true;
    char pty_buf[4096];

    /// State for WebSocket frame reassembly (RFC 6455 fragmentation)
    String fragment_buffer;
    uint8_t fragment_opcode = 0;
    bool in_fragmented_message = false;
    static constexpr size_t MAX_MESSAGE_SIZE = 16 * 1024 * 1024;

    /// Do not set a receive timeout on the socket. We use poll() to check for
    /// readability before calling readWebSocketFrame. Setting a receive timeout
    /// would risk partial frame reads on timeout, causing protocol desync.

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

        /// Data from PTY -> send to WebSocket
        if (fds[1].revents & POLLIN)
        {
            ssize_t n = read(pty_master_fd, pty_buf, sizeof(pty_buf));
            if (n > 0)
            {
                try
                {
                    sendWebSocketBinary(socket, pty_buf, static_cast<size_t>(n));
                }
                catch (...)
                {
                    LOG_DEBUG(log, "Failed to send to WebSocket: {}", getCurrentExceptionMessage(false));
                    running = false;
                }
            }
            else if (n == 0)
            {
                running = false;
            }
        }

        /// Data from WebSocket -> send to PTY
        if (fds[0].revents & POLLIN)
        {
            try
            {
                WebSocketFrame frame = readWebSocketFrame(socket);

                if (frame.protocol_error)
                {
                    sendWebSocketClose(socket, 1002, "Protocol error");
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
                            sendWebSocketClose(socket, 1000, "Bye");
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
                        sendWebSocketClose(socket, 1002, "Protocol error: new message during fragmentation");
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
                        sendWebSocketClose(socket, 1002, "Protocol error: unexpected continuation frame");
                        running = false;
                        continue;
                    }
                    if (fragment_buffer.size() + frame.payload.size() > MAX_MESSAGE_SIZE)
                    {
                        LOG_WARNING(log, "WebSocket message exceeded size limit");
                        sendWebSocketClose(socket, 1009, "Message too big");
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
                        {
                            try
                            {
                                client_runner->changeWindowSize(cols, rows, 0, 0);
                            }
                            catch (...)
                            {
                                LOG_DEBUG(log, "Failed to resize PTY: {}", getCurrentExceptionMessage(false));
                            }
                        }
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
                                    continue;
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
            catch (const Poco::TimeoutException &) // NOLINT(bugprone-empty-catch)
            {
                /// Timeout reading frame, continue polling
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

    /// Drain remaining PTY output
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

    /// Send WebSocket close frame
    try
    {
        sendWebSocketClose(socket, 1000, "Session ended");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to send WebSocket close frame");
    }

    /// Shutdown the socket so the HTTP connection loop exits cleanly
    try
    {
        socket.shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to shutdown WebSocket");
    }

    LOG_INFO(log, "WebSocket connection closed");
}


void WebTerminalRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    if (!server.config().getBool("allow_experimental_webterminal", false))
    {
        response.setContentType("text/plain; charset=UTF-8");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        *response.send() << "Web terminal is disabled. See the `allow_experimental_webterminal` server configuration.\n";
        return;
    }

    /// Check if this is a WebSocket upgrade request
    String connection = request.get("Connection", "");
    std::transform(connection.begin(), connection.end(), connection.begin(), ::tolower);

    String upgrade = request.get("Upgrade", "");
    std::transform(upgrade.begin(), upgrade.end(), upgrade.begin(), ::tolower);

    if (connection.find("upgrade") != String::npos && upgrade == "websocket")
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
