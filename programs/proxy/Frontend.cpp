#include <Frontend.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Core/Defines.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/Net/SocketAddress.h>

#include <algorithm>
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

namespace
{

/// A `WITH` clause may introduce a `SELECT` or an `INSERT`, and `select` and `insert` may also be names within it
/// (`WITH 1 AS select, 1 + select AS x INSERT ...`), so the type of such a query is determined with the server's grammar.
String classifyWithQuery(std::string_view query, bool is_prefix)
{
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end);
    String error;
    ASTPtr ast = tryParseQuery(parser, begin, end, error, /*hilite=*/ false, "query", /*allow_multi_statements=*/ false,
        /*max_query_size=*/ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, /*skip_insignificant=*/ true);

    /// A query that the server would not parse, or a prefix that does not parse yet, has no known type.
    if (!ast)
        return "";

    if (const auto * insert = ast->as<ASTInsertQuery>())
    {
        /// Once the parser reached the inline data of an `INSERT`, the rest of a prefix cannot change the statement.
        if (is_prefix && !insert->data)
            return "";
        return "insert";
    }

    /// The rest of a prefix may still turn the query into a write.
    if (is_prefix)
        return "";

    if (ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>() || ast->as<ASTSelectIntersectExceptQuery>())
        return "select";
    return "other";
}

}

String classifyQuery(std::string_view query, bool is_prefix)
{
    /// Tokenized with the server's lexer, so that comments (nested, `--`, `//`, `#`) and literals
    /// are recognized exactly as the server does.
    Lexer lexer(query.data(), query.data() + query.size());
    auto next_significant = [&]
    {
        while (true)
        {
            Token token = lexer.nextToken();
            if (token.isSignificant())
                return token;
        }
    };

    /// A token reaching the end of a prefix may continue past it: `/` may start a comment, `INS` may be `INSERT`.
    auto is_cut = [&](const Token & token)
    {
        return is_prefix && (token.isEnd() || token.end == query.data() + query.size());
    };

    auto is_keyword = [](const Token & token, std::string_view keyword)
    {
        return token.type == TokenType::BareWord && token.size() == keyword.size()
            && std::equal(token.begin, token.end, keyword.begin(),
                [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == b; });
    };

    const Token first = next_significant();
    if (is_cut(first))
        return "";
    if (is_keyword(first, "insert"))
        return "insert";

    if (is_keyword(first, "with"))
        return classifyWithQuery(query, is_prefix);

    if (!is_keyword(first, "select") && !is_keyword(first, "show") && !is_keyword(first, "describe")
        && !is_keyword(first, "desc") && !is_keyword(first, "explain") && !is_keyword(first, "exists"))
        return "other";

    /// A read query is not established by its first keyword alone: a read statement may be combined with a write
    /// into `statement1 PARALLEL WITH statement2`. Look at the rest of the query outside of brackets.
    size_t depth = 0;
    bool after_parallel = false;
    while (true)
    {
        const Token token = next_significant();
        if (token.isEnd() || token.isError() || is_cut(token))
            break;

        const bool parallel = depth == 0 && is_keyword(token, "parallel");
        if (token.type == TokenType::OpeningRoundBracket || token.type == TokenType::OpeningSquareBracket)
        {
            ++depth;
        }
        else if (token.type == TokenType::ClosingRoundBracket || token.type == TokenType::ClosingSquareBracket)
        {
            if (depth > 0)
                --depth;
        }
        else if (depth == 0 && after_parallel && is_keyword(token, "with"))
        {
            return "other";
        }
        after_parallel = parallel;
    }

    /// The rest of a prefix may still turn the query into a write.
    if (is_prefix)
        return "";
    return "select";
}

FiberSocket connectToBackend(const FrontendContext & ctx, Backend & backend, bool encrypt)
{
    if (encrypt && (ctx.listener.protocol == ListenerProtocol::MySQL || ctx.listener.protocol == ListenerProtocol::PostgreSQL))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Backend {} is secure, but the {} protocol negotiates TLS in-band and the proxy does not support "
            "a TLS backend leg for it", backend.name(), toString(ctx.listener.protocol));

    const UInt16 port = backendPortFor(ctx.listener.protocol, backend.config(), ctx.listener.port);

    const auto started = std::chrono::steady_clock::now();
    try
    {
        /// Inside the guarded section: a backend whose name stops resolving is failing like an unreachable one.
        const Poco::Net::SocketAddress address = resolveAddress(backend.config().host, port);

        FiberSocket socket;
        if (encrypt)
        {
#if USE_SSL
            /// The backend leg is a connection to the backend, so it announces - and, when
            /// `openSSL.client.extendedVerification` is enabled, is verified against - the backend's own name.
            /// Reusing the name the client asked for (an HTTP `Host` header, a client SNI) would verify
            /// the backend against the proxy's public identity instead.
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
        if (decision.dynamic_backends_exhausted)
            result.failure_reason = "a routing rule matched, but the limit of dynamic backends (`max_dynamic_backends`) is reached";
        else if (decision.user_unknown)
            result.failure_reason = "the user name is empty, but the routing rule to apply matches on it";
        else
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
