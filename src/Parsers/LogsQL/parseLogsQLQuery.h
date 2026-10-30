#pragma once

#include <Parsers/IParser.h>

namespace DB
{

/// Counterparts of tryParseQuery/parseQuery for the LogsQL dialect.
///
/// The standard tryParseQuery validates the query with the ClickHouse lexer: it fails on lexical errors
/// and on unmatched brackets. Valid LogsQL queries routinely violate both (e.g. `_time:[2023-01-01, 2023-02-01)`
/// uses intentionally unbalanced brackets), so these functions skip those checks
/// and fully trust the validation done by ParserLogsQLQuery itself.

ASTPtr tryParseLogsQLQuery(
    IParser & parser,
    const char * & _out_query_end, /// The query begin as an input parameter.
    const char * all_queries_end,
    String & out_error_message,
    int * out_error_code,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

ASTPtr parseLogsQLQueryAndMovePosition(
    IParser & parser,
    const char * & pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

ASTPtr parseLogsQLQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

}
