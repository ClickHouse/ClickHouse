#include <Frontend.h>

#if USE_SILK

#include <Relay.h>

#include <StatusPage.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <base/scope_guard.h>

#include <silk/fibers/fiber.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <fmt/format.h>

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
    std::optional<String> param_database;
    std::optional<String> param_session_id;
    std::optional<String> param_query;

    Poco::URI uri(target);
    for (const auto & [key, value] : uri.getQueryParameters())
    {
        if (key == "user")
            assign_first(param_user, value);
        else if (key == "database")
            assign_first(param_database, value);
        else if (key == "session_id")
            assign_first(param_session_id, value);
        else if (key == "query")
            assign_first(param_query, value);
    }

    /// Read the request headers.
    /// RecordingReader keeps every byte received so far to replay the request head to the backend,
    /// so the per-line limit alone does not bound memory: cap the total size of the request head,
    /// or a client could stream an unbounded number of headers before a backend is even chosen.
    constexpr size_t max_request_head_bytes = 1024 * 1024;
    std::optional<String> header_host;
    std::optional<String> header_user;
    std::optional<String> header_database;
    std::optional<String> basic_auth_user;
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

        if (name == "host")
            assign_first(header_host, Poco::toLower(stripPort(value)));   /// DNS hostnames are case-insensitive.
        else if (name == "x-clickhouse-user")
            assign_first(header_user, value);
        else if (name == "x-clickhouse-database")
            assign_first(header_database, value);
        else if (name == "authorization" && value.starts_with("Basic ") && !basic_auth_user)
        {
            try
            {
                const String decoded = base64Decode(value.substr(6));
                const size_t sep = decoded.find(':');
                if (sep != String::npos)
                    basic_auth_user = decoded.substr(0, sep);
            }
            catch (...)  // NOLINT(bugprone-empty-catch)
            {
                /// A malformed Authorization header is Ok to ignore: the request is routed without a user.
            }
        }
    }

    /// The same precedence as in `authenticateUserByHTTP`: the `X-ClickHouse-User` header wins,
    /// then the query parameters, and the `Authorization` header is only used if neither is present.
    attributes.host = header_host.value_or("");
    attributes.database = header_database.value_or(param_database.value_or(""));
    attributes.session_id = param_session_id.value_or("");
    if (param_query)
        attributes.query_type = classifyQuery(*param_query);

    if (header_user)
        attributes.user = *header_user;
    else if (param_user)
        attributes.user = *param_user;
    else if (basic_auth_user)
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
