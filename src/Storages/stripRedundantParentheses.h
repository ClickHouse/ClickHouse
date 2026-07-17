#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

class IAST;

/// Recursively clears the `parenthesized` flag on `ast` and all of its descendants, except for
/// aliased expressions: `(expr AS alias)` keeps the parentheses because without them the alias
/// could change the meaning of the surrounding clause on reparse.
///
/// Storage metadata (keys, TTLs, column defaults, indices, projections, constraints) must be
/// canonical: it is serialized (into ZooKeeper and the `columns`/`metadata` strings) and compared
/// as formatted strings, and versions before 26.5 never preserved user-written parentheses in it.
/// Apply this to a definition AST when it is read into a storage description, so that
/// semantically identical definitions written with and without redundant parentheses
/// (`PARTITION BY (a)` vs `PARTITION BY a`) produce identical metadata.
void stripRedundantParentheses(IAST & ast);

}
