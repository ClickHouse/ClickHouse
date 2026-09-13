#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/** Rewrites Trino function calls in the AST into their ClickHouse equivalents.
  *
  * Three kinds of rewrites are performed (see the tables in TrinoFunctionMapper.cpp):
  * - simple renames with the same argument order, e.g. `strpos` -> `positionUTF8`;
  * - argument restructuring, e.g. `transform(arr, f)` -> `arrayMap(f, arr)`
  *   (Trino passes lambdas last, ClickHouse first), or `approx_percentile(x, p)`
  *   -> `quantileTDigest(p)(x)` (the percentile becomes an aggregate parameter);
  * - names that resolve in ClickHouse with different semantics, e.g. Trino
  *   `length` counts code points while ClickHouse `length` counts bytes, so it
  *   is mapped to `lengthUTF8`.
  *
  * Function names that are not known Trino functions are left untouched, so
  * native ClickHouse functions remain accessible from the Trino dialect.
  */
void mapTrinoFunctions(ASTPtr & ast);

}
