#include <Parsers/LogsQL/ParserLogsQLQuery.h>

#include <Parsers/LogsQL/LogsQLParser.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int SUPPORT_IS_DISABLED;
}

bool ParserLogsQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// SET queries are parsed with the normal ClickHouse parser, so that settings
    /// like `dialect` and `logsql_table` can always be changed. This is checked before
    /// the feature gate so users can recover from misconfigured profiles.
    ParserSetQuery set_parser;
    if (set_parser.parse(pos, node, expected))
        return true;

    if (!feature_enabled)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Support for the LogsQL dialect is disabled (turn on setting 'allow_experimental_logsql_dialect')");

    if (table.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "The name of the logs table to use with the logsql dialect is not specified, use: SET logsql_table = '...'");

    const char * begin = pos->begin;

    /// The LogsQL text is scanned from the raw query string, bypassing the ClickHouse
    /// token stream (which is bounded by `max_query_size` on its own), so the limit
    /// must be applied to the raw slice as well. The budget is measured from the raw
    /// query begin (before the leading whitespace and comments skipped by the token
    /// stream), the same way the SQL path measures it. The end is clipped rather than
    /// checked against the whole slice, because in a multi-statement input the slice
    /// extends to the end of all statements, while the limit applies to a single query.
    const char * end = raw_end;
    bool truncated = false;
    if (max_query_size && static_cast<size_t>(raw_end - raw_begin) > max_query_size)
    {
        end = std::max(begin, raw_begin + max_query_size);
        truncated = true;
    }

    LogsQLParser::Context context;
    context.database = database;
    context.table = table;
    context.time_column = time_column;
    context.msg_column = msg_column;
    context.max_depth = max_parser_depth;
    context.truncated = truncated;

    LogsQLParser parser(begin, end, std::move(context));
    node = parser.parse();

    /// Advance the token iterator to the end of the parsed LogsQL text,
    /// so that the caller can detect the end of the query (a semicolon or the end of input).
    const char * parsed_end = parser.getParsedEnd();
    while (!pos->isEnd() && pos->begin < parsed_end)
        ++pos;

    return true;
}

}
