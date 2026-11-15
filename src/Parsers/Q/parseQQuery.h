#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{

/** Parses one query of the `q` dialect starting at `pos`, and advances `pos` past it.
  *
  * In q, `;` separates the statements of a program, so `a:1; a+1` is one query. A query ends at
  * a top-level `;` that is the last significant character of its line (or at the end of the text).
  * This keeps scripts like `select from t;\nselect from u;` working as two queries.
  *
  * The result is an `ASTQQuery` with the parsed program, or an `ASTSetQuery` for a plain `SET`,
  * which is always recognized, so that the dialect can be switched back.
  *
  * `--` comments (after whitespace or at the start of a line) are accepted in addition to the q
  * comments, so that the queries can be annotated in SQL test files.
  */
ASTPtr parseQQuery(
    const char *& pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

/** Parses the query at `pos` only if it is a plain `SET` query, and returns `nullptr` otherwise.
  * This is used when the `q` dialect is disabled, so that a session in `dialect = 'q'` can still switch back.
  */
ASTPtr tryParseQSetQuery(
    const char *& pos,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks);

/// Whether only whitespace and comments (in the sense of the `q` dialect) are left from `pos` to `end`.
bool isQQueryTextEmpty(const char * pos, const char * end);

/// Where the next query of the `q` dialect starts, after the query at `pos` (used to skip a query that failed to parse).
const char * skipQQuery(const char * pos, const char * end);

}
