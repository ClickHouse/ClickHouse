#pragma once

#include <Parsers/Lexer.h>
#include <base/types.h>

#include <optional>
#include <vector>

namespace DB
{

/** Translates Trino-specific SQL syntax into ClickHouse SQL at the token level.
  *
  * The input is the significant tokens of a single statement (as produced by the
  * ClickHouse Lexer) together with the original text bounds. Constructs that the
  * ClickHouse parser understands directly (which is most of Trino's syntax) are
  * passed through unchanged. The following constructs are rewritten:
  *
  * - `ARRAY[1, 2, 3]`                    -> `[1, 2, 3]`
  * - `TRY_CAST(x AS t)`                  -> `accurateCastOrNull(x, 't')`
  * - `ROW(...)` constructor              -> `tuple(...)`
  * - `ROW(...)`, `ARRAY(...)`, `MAP(...)` types in `CAST` -> `Tuple(...)`, `Array(...)`, `Map(...)`
  * - `TIMESTAMP(p) [WITH TIME ZONE]` type -> `DateTime64(p)`
  * - `UNNEST` in the `FROM` clause       -> `ARRAY JOIN` / `LEFT ARRAY JOIN` / a subquery with `arrayJoin`
  * - `OFFSET n LIMIT m`                  -> `LIMIT m OFFSET n`
  * - `FETCH FIRST n ROWS ONLY/WITH TIES` -> `LIMIT n [WITH TIES]`
  * - `LIMIT ALL`                         -> removed
  * - `VALUES row, row, ...` statement    -> `SELECT * FROM SQLStandardValues(row, row, ...)`
  * - `SET SESSION name = value`          -> `SET name = value`
  * - `length`, `substr`, `substring`, `lpad`, `rpad` over a syntactically
  *   recognizable VARBINARY expression -> the byte-based ClickHouse functions
  *   (the VARCHAR overloads count code points and are mapped to the UTF8
  *   variants by TrinoFunctionMapper)
  * - backslashes in string literals are escaped, because in Trino a backslash is
  *   a regular character while ClickHouse processes escape sequences
  *
  * Returns std::nullopt if the statement does not need any translation, in which
  * case the caller should parse the original text (this keeps zero-copy pointers
  * of `INSERT` data valid).
  *
  * Function names are translated separately, on the AST (see TrinoFunctionMapper.h).
  */
std::optional<String> translateTrinoSyntax(const std::vector<Token> & tokens, const char * begin, const char * end);

}
