#if defined(OS_LINUX)

#include <Server/WebTerminalRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTPHandler.h>
#include <Server/HTTPResponseHeaderWriter.h>
#include <Server/IServer.h>
#include <Server/ClientEmbedded/ClientEmbeddedRunner.h>
#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Access/Credentials.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/SHA1Engine.h>
#include <Poco/Base64Encoder.h>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <sstream>


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

    std::ostringstream oss;
    Poco::Base64Encoder encoder(oss);
    encoder.write(reinterpret_cast<const char *>(digest.data()), static_cast<std::streamsize>(digest.size()));
    encoder.close();
    return oss.str();
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
    socket.sendBytes(buf.data(), static_cast<int>(buf.size()));
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

    bool masked = (header[1] & 0x80) != 0;
    uint64_t payload_len = header[1] & 0x7F;

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
    if (masked)
    {
        if (!readExact(socket, reinterpret_cast<char *>(mask_key), 4))
            return frame;
    }

    frame.payload.resize(payload_len);
    if (payload_len > 0)
    {
        if (!readExact(socket, frame.payload.data(), payload_len))
            return frame;

        if (masked)
        {
            for (uint64_t i = 0; i < payload_len; ++i)
                frame.payload[i] ^= static_cast<char>(mask_key[i % 4]);
        }
    }

    frame.valid = true;
    return frame;
}

/// Parse a simple JSON message like {"type":"resize","cols":80,"rows":24}
/// This is a minimal parser sufficient for our control messages.
bool parseResizeMessage(const String & json, int & cols, int & rows)
{
    /// Look for "type":"resize"
    if (json.find("\"resize\"") == String::npos)
        return false;

    static constexpr int MAX_TERMINAL_DIMENSION = 500;

    auto extract_int = [&](const char * key) -> int
    {
        auto pos = json.find(key);
        if (pos == String::npos)
            return -1;
        pos += strlen(key);
        /// Skip to the colon and whitespace
        pos = json.find(':', pos);
        if (pos == String::npos)
            return -1;
        ++pos;
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
            ++pos;
        int value = 0;
        while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9')
        {
            value = value * 10 + (json[pos] - '0');
            if (value > MAX_TERMINAL_DIMENSION)
                return MAX_TERMINAL_DIMENSION;
            ++pos;
        }
        return value;
    };

    cols = extract_int("\"cols\"");
    rows = extract_int("\"rows\"");
    return cols > 0 && rows > 0;
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

    /// Validate WebSocket upgrade headers per RFC 6455
    String ws_key = request.get("Sec-WebSocket-Key", "");
    if (ws_key.empty())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << "Missing Sec-WebSocket-Key.\n";
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

    /// Authenticate the user using the same mechanism as HTTP
    auto session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP, request.isSecure());
    std::unique_ptr<Credentials> request_credentials;
    const auto & default_settings = server.context()->getSettingsRef();
    HTMLForm params(default_settings, request);
    HTTPHandlerConnectionConfig connection_config;

    try
    {
        if (!authenticateUserByHTTP(request, params, response, *session, request_credentials, connection_config, server.context(), log))
        {
            /// Multi-step auth (401 already sent)
            return;
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "WebSocket authentication failed: {}", getCurrentExceptionMessage(false));
        /// Send WebSocket-compatible error: complete the handshake first then close with error code
        /// Actually, since handshake hasn't completed, just return an HTTP error
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
        *response.send() << "Authentication failed.\n";
        return;
    }

    /// Complete the WebSocket handshake
    Poco::Net::StreamSocket & socket = response.getSocket();
    String accept_key = computeWebSocketAccept(ws_key);

    std::ostringstream handshake;
    handshake << "HTTP/1.1 101 Switching Protocols\r\n"
              << "Upgrade: websocket\r\n"
              << "Connection: Upgrade\r\n"
              << "Sec-WebSocket-Accept: " << accept_key << "\r\n"
              << "\r\n";
    String handshake_str = handshake.str();
    socket.sendBytes(handshake_str.data(), static_cast<int>(handshake_str.size()));

    LOG_INFO(log, "WebSocket connection established for user {}", session->getClientInfo().current_user);

    /// Create PTY for the embedded client
    static constexpr int DEFAULT_COLS = 80;
    static constexpr int DEFAULT_ROWS = 24;

    auto pty_descriptors = std::make_unique<PtyClientDescriptorSet>("xterm-256color", DEFAULT_COLS, DEFAULT_ROWS, 0, 0);
    auto client_runner = std::make_unique<ClientEmbeddedRunner>(std::move(pty_descriptors), std::move(session));

    NameToNameMap envs;
    client_runner->run(envs);

    auto server_descriptors = client_runner->getDescriptorsForServer();
    int pty_master_fd = server_descriptors.out;

    /// Keep the socket in blocking mode. We use poll() to check for data availability
    /// before reading, and send operations are always blocking.
    /// This avoids exception-safety issues with toggling blocking mode.

    /// Make PTY master non-blocking for polling
    int flags = fcntl(pty_master_fd, F_GETFL, 0);
    fcntl(pty_master_fd, F_SETFL, flags | O_NONBLOCK);

    /// Main I/O loop: bridge WebSocket <-> PTY
    bool running = true;
    char pty_buf[4096];

    /// State for WebSocket frame reassembly (RFC 6455 fragmentation)
    String fragment_buffer;
    uint8_t fragment_opcode = 0;

    /// Set a receive timeout so readWebSocketFrame doesn't block forever
    socket.setReceiveTimeout(Poco::Timespan(0, 200000)); /// 200ms

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
                    /// First frame of a message (text or binary)
                    fragment_opcode = frame.opcode;
                    fragment_buffer = std::move(frame.payload);
                }
                else
                {
                    /// Continuation frame
                    fragment_buffer.append(frame.payload);
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
                            if (written <= 0)
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
            break;
        }
    }

    /// Send WebSocket close frame
    try
    {
        sendWebSocketClose(socket, 1000, "Session ended");
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    /// Shutdown the socket so the HTTP connection loop exits cleanly
    try
    {
        socket.shutdown();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    LOG_INFO(log, "WebSocket connection closed");
}


void WebTerminalRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
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
        serveHTML(request, response);
    }
}

}

#endif
