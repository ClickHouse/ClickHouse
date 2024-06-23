#include <string>
#include <Parsers/PRQL/ParserPRQLQuery.h>

#include "Parsers/Lexer.h"
#include "config.h"

#if USE_PRQL
#    include <prql.h>
#endif

#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

bool ParserPRQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserSetQuery set_p;

    if (set_p.parse(pos, node, expected))
        return true;

#if !USE_PRQL
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED, "PRQL is not available. Rust code or PRQL itself may be disabled. Use another dialect!");
#else
    const auto * begin = pos->begin;

    // The same parsers are used in the client and the server, so the parser have to detect the end of a single query in case of multiquery queries
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
        ++pos;

    const auto * end = pos->begin;

    uint8_t * sql_query_ptr{nullptr};
    uint64_t sql_query_size{0};

    const auto res
        = prql_to_sql(reinterpret_cast<const uint8_t *>(begin), static_cast<uint64_t>(end - begin), &sql_query_ptr, &sql_query_size);

    SCOPE_EXIT({ prql_free_pointer(sql_query_ptr); });

    const auto * sql_query_char_ptr = reinterpret_cast<char *>(sql_query_ptr);
    const auto * const original_sql_query_ptr = sql_query_char_ptr;

    if (res != 0)
    {
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PRQL syntax error: '{}'", sql_query_char_ptr);
    }
    chassert(sql_query_size > 0);

    ParserQuery query_p(end, false);
    String error_message;
    node = tryParseQuery(
        query_p,
        sql_query_char_ptr,
        sql_query_char_ptr + sql_query_size - 1,
        error_message,
        false,
        "",
        false,
        max_query_size,
        max_parser_depth,
        max_parser_backtracks,
        true);

    if (!node)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Error while parsing the SQL query generated from PRQL query :'{}'.\nPRQL Query:'{}'\nSQL query: '{}'",
            error_message,
            std::string_view{begin, end},
            std::string_view(original_sql_query_ptr, original_sql_query_ptr + sql_query_size));


    return true;
#endif
}
}
