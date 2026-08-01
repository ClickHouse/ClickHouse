#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{

/** Parses one KQL statement starting at `pos` and returns it as a ClickHouse AST.
  *
  * `pos` is advanced past the statement that was consumed, so a script such as
  * `T | take 3; SET dialect = 'clickhouse'` leaves the `SET` for the next call.
  * `let` statements are not statements in that sense: they bind names for the tabular
  * expression that follows and are consumed together with it.
  *
  * Throws `Exception` on any problem - there is no `try` variant, because callers that
  * need to turn a parse failure into a message (the interactive client) already catch,
  * and `src/Parsers` is not allowed to.
  */
ASTPtr parseKQLQuery(
    const char *& pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

}
