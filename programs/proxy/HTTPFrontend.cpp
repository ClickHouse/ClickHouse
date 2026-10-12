#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <StatusPage.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <base/scope_guard.h>

#include <silk/fibers/fiber.h>

#include <Poco/Ascii.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <fmt/format.h>

#include <algorithm>
#include <optional>


namespace DB::Proxy
{

namespace
{

void sendResponse(FiberSocket & client, int code, const String & reason, const String & content_type, const String & body)
{
    const String data = fmt::format(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        code, reason, content_type, body.size(), body);
    try
    {
        client.sendAll(data.data(), data.size());
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        /// The client may have disconnected before reading the response; that is Ok.
    }
}

String stripPort(const String & host)
{
    /// Strip ":port" but keep bracketed IPv6 literals intact.
    if (!host.empty() && host.front() == '[')
    {
        const size_t bracket = host.find(']');
        return bracket == String::npos ? host : host.substr(0, bracket + 1);
    }
    const size_t colon = host.rfind(':');
    return colon == String::npos ? host : host.substr(0, colon);
}

/// The framing of an HTTP request body, from the request headers.
struct HTTPBodyFraming
{
    std::optional<UInt64> content_length;
    bool chunked = false;
    bool encoded = false;               /// A `Content-Encoding` other than `identity`: the body cannot be read as text.
    bool expect_continue = false;       /// The client waits for `100 Continue` before sending the body.
};

struct QueryPrefix
{
    String data;
    bool is_prefix = false;     /// Only the beginning of the body was read: it is longer, or the rest is in later chunks.
};

/// Without a `query` parameter, the body of the request is the query. Reads the beginning of the body
/// (the bytes stay in the reader's buffer and are forwarded to the backend unchanged) to classify the query.
/// Returns nothing if the body cannot be inspected: it is compressed, or its framing is not known.
std::optional<QueryPrefix> readQueryPrefixFromBody(FiberSocket & client, RecordingReader & reader, const HTTPBodyFraming & framing)
{
    /// Enough for the leading keyword after a reasonable amount of whitespace and comments.
    constexpr size_t max_prefix_bytes = 64 * 1024;

    if (framing.encoded || (!framing.chunked && !framing.content_length))
        return std::nullopt;

    if (framing.expect_continue)
    {
        /// Without this, the client does not send the body until the backend answers, which it cannot do
        /// before the proxy chooses it. The backend answers `100 Continue` once more: a client must accept
        /// any number of interim responses.
        static constexpr std::string_view response = "HTTP/1.1 100 Continue\r\n\r\n";
        client.sendAll(response.data(), response.size());
    }

    UInt64 size = 0;
    if (framing.chunked)
    {
        /// Only the first chunk is inspected: chunk-size [ ";" chunk-ext ] CRLF chunk-data.
        String chunk_header;
        if (!reader.readLine(chunk_header, 1024))
            return std::nullopt;
        const String hex = Poco::trim(chunk_header.substr(0, chunk_header.find(';')));
        if (hex.empty() || hex.size() > 15 || hex.find_first_not_of("0123456789abcdefABCDEF") != String::npos)
            return std::nullopt;
        size = std::stoull(hex, nullptr, 16);
    }
    else
    {
        size = *framing.content_length;
    }

    const size_t length = std::min<UInt64>(size, max_prefix_bytes);
    if (!reader.ensure(length))
        return std::nullopt;
    /// A chunked body may continue in the next chunks, even if they are empty.
    return QueryPrefix{.data = reader.readFixed(length), .is_prefix = framing.chunked || length < size};
}

const StaticPageConfig * findStaticPage(const HTTPConfig & http, const String & path)
{
    for (const auto & page : http.static_pages)
        if (page.path == path)
            return &page;
    return nullptr;
}

void serveStaticPage(FiberSocket & client, const StaticPageConfig & page)
{
    if (!page.content.empty())
    {
        sendResponse(client, 200, "OK", page.content_type, page.content);
        return;
    }

    String body;
    bool have_body = false;
    {
        /// Reading a file from disk blocks; step out of the cooperative scheduler while doing it.
        silk::FiberScheduler::ThreadModeScope thread_mode;
        try
        {
            ReadBufferFromFile file(page.file);
            readStringUntilEOF(body, file);
            have_body = true;
        }
        catch (...)  // NOLINT(bugprone-empty-catch)
        {
            /// A missing or unreadable file is Ok: it is reported as 404 below.
        }
    }

    if (have_body)
        sendResponse(client, 200, "OK", page.content_type, body);
    else
        sendResponse(client, 404, "Not Found", "text/plain; charset=UTF-8", "Not found\n");
}

}

void handleHTTP(FiberSocket & client, const FrontendContext & ctx)
{
    client.setTimeouts(ctx.config.handshake_timeout_ms, ctx.config.send_timeout_ms);

    RecordingReader reader(client);

    String request_line;
    if (!reader.readLine(request_line, 64 * 1024) || request_line.empty())
        return;

    /// Method SP request-target SP HTTP-version.
    const size_t method_end = request_line.find(' ');
    const String method = request_line.substr(0, method_end);
    String target;
    if (method_end != String::npos)
    {
        const size_t target_begin = request_line.find_first_not_of(' ', method_end);
        if (target_begin != String::npos)
        {
            const size_t target_end = request_line.find(' ', target_begin);
            target = request_line.substr(target_begin, target_end - target_begin);
        }
    }
    if (method.empty() || target.empty())
    {
        sendResponse(client, 400, "Bad Request", "text/plain; charset=UTF-8", "Bad request\n");
        return;
    }

    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::HTTP;
    attributes.peer_address = client.peerAddress().host().toString();

    /// The server resolves a repeated query parameter or header to its *first* occurrence
    /// (`Poco::Net::NameValueCollection::get`), so the proxy has to route by the first occurrence too.
    /// Otherwise a client could send `?user=a&user=b`, make the proxy route by `b`,
    /// and have the backend authenticate and execute the query as `a`.
    auto assign_first = [](std::optional<String> & destination, const String & value)
    {
        if (!destination)
            destination = value;
    };

    std::optional<String> param_user;
    bool param_password = false;
    std::optional<String> param_database;
    std::optional<String> param_session_id;
    std::optional<String> param_query;
    bool param_decompress = false;

    Poco::URI uri(target);
    for (const auto & [key, value] : uri.getQueryParameters())
    {
        if (key == "user")
            assign_first(param_user, value);
        else if (key == "password")
            param_password = true;
        else if (key == "database")
            param_database = value;     /// Unlike the above, a setting: the server applies them in order, so the last one wins.
        else if (key == "session_id")
            assign_first(param_session_id, value);
        else if (key == "query")
            assign_first(param_query, value);
        else if (key == "decompress")
            param_decompress |= value != "0" && !value.empty();
    }

    /// Read the request headers.
    /// RecordingReader keeps every byte received so far to replay the request head to the backend,
    /// so the per-line limit alone does not bound memory: cap the total size of the request head,
    /// or a client could stream an unbounded number of headers before a backend is even chosen.
    constexpr size_t max_request_head_bytes = 1024 * 1024;
    std::optional<String> header_host;
    std::optional<String> header_user;
    std::optional<String> header_database;
    std::optional<String> header_authorization;
    bool seen_content_type = false;
    bool multipart_form_data = false;
    HTTPBodyFraming body_framing;
    String header;
    while (reader.readLine(header, 64 * 1024) && !header.empty())
    {
        if (reader.received().size() > max_request_head_bytes)
        {
            sendResponse(client, 431, "Request Header Fields Too Large", "text/plain; charset=UTF-8",
                "The request head exceeds " + std::to_string(max_request_head_bytes) + " bytes\n");
            return;
        }

        const size_t colon = header.find(':');
        if (colon == String::npos)
            continue;
        const String name = Poco::toLower(Poco::trim(header.substr(0, colon)));
        const String value = Poco::trim(header.substr(colon + 1));

        if (name == "content-length")
        {
            UInt64 content_length = 0;
            if (!body_framing.content_length && tryParse(content_length, value))
                body_framing.content_length = content_length;
        }
        else if (name == "transfer-encoding")
            body_framing.chunked |= Poco::toLower(value).contains("chunked");
        else if (name == "content-encoding")
            body_framing.encoded |= !value.empty() && Poco::icompare(value, "identity") != 0;
        else if (name == "expect")
            body_framing.expect_continue |= Poco::icompare(value, "100-continue") == 0;
        else if (name == "host")
            assign_first(header_host, Poco::toLower(stripPort(value)));   /// DNS hostnames are case-insensitive.
        else if (name == "content-type")
        {
            if (!seen_content_type)
                multipart_form_data = Poco::toLower(value).starts_with("multipart/form-data");
            seen_content_type = true;
        }
        else if (name == "x-clickhouse-user")
            assign_first(header_user, value);
        else if (name == "x-clickhouse-database")
            assign_first(header_database, value);
        else if (name == "authorization")
            assign_first(header_authorization, value);
    }

    /// The user from the `Authorization` header, parsed as in `authenticateUserByHTTP`:
    /// `Poco::Net::HTTPRequest::getCredentials` splits the first header into a scheme and the rest
    /// at any whitespace, and the scheme is compared case-insensitively.
    std::optional<String> basic_auth_user;
    if (header_authorization && *header_authorization != "never")
    {
        const String & value = *header_authorization;
        const auto is_space = [](char c) { return Poco::Ascii::isSpace(c); };
        const auto scheme_end = std::find_if(value.begin(), value.end(), is_space);
        const auto info_begin = std::find_if_not(scheme_end, value.end(), is_space);
        if (Poco::icompare(String(value.begin(), scheme_end), "Basic") == 0)
        {
            try
            {
                basic_auth_user = Poco::Net::HTTPBasicCredentials(String(info_begin, value.end())).getUsername();
            }
            catch (...)  // NOLINT(bugprone-empty-catch)
            {
                /// A malformed Authorization header is Ok to ignore: the request is routed without a user.
            }
        }
    }

    /// The server ignores empty `X-ClickHouse-User` and `X-ClickHouse-Database` headers.
    if (header_user && header_user->empty())
        header_user.reset();
    if (header_database && header_database->empty())
        header_database.reset();

    /// The same precedence as in `authenticateUserByHTTP`: the `X-ClickHouse-User` header wins,
    /// then the query parameters, and the `Authorization` header is only used if neither
    /// a `user` nor a `password` parameter is present.
    attributes.host = header_host.value_or("");
    attributes.database = header_database.value_or(param_database.value_or(""));
    attributes.session_id = param_session_id.value_or("");

    /// As in `DynamicQueryHandler::getQuery`, the query is the first `query` URL parameter, a line break,
    /// and then the request body (`ConcatReadBuffer`), so the parameter and the body are classified together.
    /// With `multipart/form-data`, the query is concatenated from all the `query` parameters of the URL
    /// and of the form, so it is not classified.
    const bool has_body = body_framing.chunked || body_framing.content_length.value_or(0) > 0;
    if (ctx.router.needsQueryType(ListenerProtocol::HTTP) && !multipart_form_data && (param_query || has_body))
    {
        String query_head;
        if (param_query && !param_query->empty())
            query_head = *param_query + "\n";

        bool is_prefix = false;
        if (has_body)
        {
            /// With `decompress=1` the body is in the compressed native format and cannot be inspected.
            std::optional<QueryPrefix> body_prefix;
            if (!param_decompress)
                body_prefix = readQueryPrefixFromBody(client, reader, body_framing);

            if (body_prefix)
            {
                query_head += body_prefix->data;
                is_prefix = body_prefix->is_prefix;
            }
            else
            {
                is_prefix = true;
            }
        }
        attributes.query_type = classifyQuery(query_head, is_prefix);
    }

    if (header_user)
        attributes.user = *header_user;
    else if (param_user)
        attributes.user = *param_user;
    else if (basic_auth_user && !param_password)
        attributes.user = *basic_auth_user;

    /// Endpoints the proxy serves itself, without a user or a backend.
    const String & path = uri.getPath();
    if (!ctx.config.http.ping_path.empty() && path == ctx.config.http.ping_path)
    {
        sendResponse(client, 200, "OK", "text/plain; charset=UTF-8", "Ok.\n");
        return;
    }
    if (!ctx.config.http.status_path.empty() && path == ctx.config.http.status_path)
    {
        sendResponse(client, 200, "OK", "application/json; charset=UTF-8", buildStatusJSON(ctx.router));
        return;
    }
    if (const StaticPageConfig * page = findStaticPage(ctx.config.http, path))
    {
        serveStaticPage(client, *page);
        return;
    }

    RouteResult route = routeConnection(ctx, attributes);
    if (!route.backend)
    {
        sendResponse(client, 503, "Service Unavailable", "text/plain; charset=UTF-8",
            "No backend available: " + route.failure_reason + "\n");
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
        sendResponse(client, 502, "Bad Gateway", "text/plain; charset=UTF-8", "Cannot connect to backend\n");
        return;
    }

    /// Forward the request head (and any buffered body). Optionally announce the client address.
    String initial = reader.received();
    if (ctx.config.http.add_x_forwarded_for && !attributes.peer_address.empty())
    {
        const size_t line_end = initial.find('\n');
        if (line_end != String::npos)
            initial.insert(line_end + 1, "X-Forwarded-For: " + attributes.peer_address + "\r\n");
    }

    LOG_DEBUG(ctx.log, "Routing HTTP {} {} (host='{}', user='{}', database='{}') to backend {}",
        method, path, attributes.host, attributes.user, attributes.database, backend.name());

    runRelay(client, backend_socket, &backend, initial, ctx.config.relay_buffer_size, ctx.config.send_timeout_ms);
    client.close();
    backend_socket.close();
}

}

#endif
