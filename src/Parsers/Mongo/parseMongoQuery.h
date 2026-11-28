#pragma once

#include <IO/WriteBufferFromString.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace Mongo
{

ASTPtr tryParseMongoQuery(
    IParser & parser,
    const char *& _out_query_end, // query start as input parameter, query end as output
    const char *& end,
    std::string & out_error_message,
    bool hilite,
    const std::string & description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    bool skip_insignificant = true);


/// Parse query or throw an exception with error message.
ASTPtr parseMongoQueryAndMovePosition(
    IParser & parser,
    const char *& pos, /// Moved to end of parsed fragment.
    const char * end,
    const std::string & description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);


/** Parses a query in the Mongo dialect. `database` overrides the database named by the query
  * itself and is what the wire protocol handlers pass, since they take it from `$db`;
  * `collection` overrides the collection the same way, which lets a namespace whose collection
  * name contains a `.` - such as `fs.files` - be addressed at all.
  */
ASTPtr parseMongoQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & description,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    const std::string & database = "",
    const std::string & collection = "");

}

}
