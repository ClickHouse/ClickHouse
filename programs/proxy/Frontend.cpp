#include <Frontend.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Net/SocketAddress.h>

#include <cctype>
#include <chrono>

#if USE_SSL
#include <Poco/Net/Context.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace DB::Proxy
{

String classifyQuery(std::string_view query)
{
    size_t i = 0;
    /// Skip leading whitespace and SQL comments.
    while (i < query.size())
    {
        if (std::isspace(static_cast<unsigned char>(query[i])))
        {
            ++i;
        }
        else if (query.compare(i, 2, "--") == 0)
        {
            while (i < query.size() && query[i] != '\n')
                ++i;
        }
        else if (query.compare(i, 2, "/*") == 0)
        {
            i += 2;
            while (i + 1 < query.size() && !(query[i] == '*' && query[i + 1] == '/'))
                ++i;
            i += 2;
        }
        else
        {
            break;
        }
    }

    size_t start = i;
    while (i < query.size() && (std::isalpha(static_cast<unsigned char>(query[i])) || query[i] == '_'))
        ++i;

    String keyword;
    for (size_t j = start; j < i; ++j)
        keyword += static_cast<char>(std::tolower(static_cast<unsigned char>(query[j])));

    if (keyword == "select" || keyword == "with" || keyword == "show" || keyword == "describe"
        || keyword == "desc" || keyword == "explain" || keyword == "exists")
        return "select";
    if (keyword == "insert")
        return "insert";
    return "other";
}

FiberSocket connectToBackend(const FrontendContext & ctx, Backend & backend, bool encrypt)
{
    const UInt16 port = backendPortFor(ctx.listener.protocol, backend.config(), ctx.listener.port);
    const Poco::Net::SocketAddress address(backend.config().host, port);

    const auto started = std::chrono::steady_clock::now();
    try
    {
        FiberSocket socket;
        if (encrypt)
        {
#if USE_SSL
            /// The backend leg is a connection to the backend, so it is verified against - and announces -
            /// the backend's own name. Reusing the name the client asked for (an HTTP `Host` header, a
            /// client SNI) would verify the backend against the proxy's public identity instead.
            socket = FiberSocket::connectTLS(
                address, ctx.config.connect_timeout_ms, ctx.client_tls_context, backend.config().host);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot connect to a secure backend: built without SSL support");
#endif
        }
        else
        {
            socket = FiberSocket::connect(address, ctx.config.connect_timeout_ms);
        }

        socket.setTimeouts(ctx.config.send_timeout_ms, ctx.config.send_timeout_ms);

        const double latency_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - started).count();
        backend.reportConnectSuccess(latency_ms);
        return socket;
    }
    catch (...)
    {
        if (ctx.router.passiveMarkingDown())
            backend.reportConnectFailure(ctx.router.failuresToMarkDown());
        else
            backend.reportError();
        throw;
    }
}

RouteResult routeConnection(const FrontendContext & ctx, const RouteAttributes & attributes)
{
    RouteResult result;

    Router::Decision decision = ctx.router.route(attributes, ctx.listener);
    result.pool = decision.pool;
    result.backend = decision.backend;

    if (!decision.pool)
    {
        result.failure_reason = "no routing rule matched and the listener has no default pool";
        LOG_WARNING(ctx.log, "Cannot route {} connection (host='{}', user='{}', database='{}'): {}",
            toString(attributes.protocol), attributes.host, attributes.user, attributes.database, result.failure_reason);
        return result;
    }

    if (!decision.backend)
    {
        result.failure_reason = "all backends of pool '" + decision.pool->name() + "' are unavailable";
        LOG_WARNING(ctx.log, "Cannot route {} connection: {}", toString(attributes.protocol), result.failure_reason);
    }

    return result;
}

}

#endif
