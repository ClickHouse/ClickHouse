#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromString.h>
namespace DB
{

/** From position in (possible multiline) query, get line number and column number in line.
  * Used in syntax error message.
  */

}
namespace DB
{

class IParser;

/// Parse query or set 'out_error_message'.
ASTPtr tryParseKQLQuery(
    IParser & parser,
    const char * & _out_query_end, // query start as input parameter, query end as output
    const char * end,
    std::string & out_error_message,
    bool hilite,
    const std::string & description,
    bool allow_multi_statements,    /// If false, check for non-space characters after semicolon and set error message if any.
    size_t max_query_size,          /// If (end - pos) > max_query_size and query is longer than max_query_size then throws "Max query size exceeded".
                                    /// Disabled if zero. Is used in order to check query size if buffer can contains data for INSERT query.
    size_t max_parser_depth,
    bool skip_insignificant = true);  /// If true, lexer will skip all insignificant tokens (e.g. whitespaces)


/// Parse query or throw an exception with error message.
ASTPtr parseKQLQueryAndMovePosition(
    IParser & parser,
    const char * & pos,                /// Moved to end of parsed fragment.
    const char * end,
    const std::string & description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth);

ASTPtr parseKQLQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & description,
    size_t max_query_size,
    size_t max_parser_depth);

ASTPtr parseKQLQuery(
    IParser & parser,
    const std::string & query,
    const std::string & query_description,
    size_t max_query_size,
    size_t max_parser_depth);

ASTPtr parseKQLQuery(
    IParser & parser,
    const std::string & query,
    size_t max_query_size,
    size_t max_parser_depth);
}
