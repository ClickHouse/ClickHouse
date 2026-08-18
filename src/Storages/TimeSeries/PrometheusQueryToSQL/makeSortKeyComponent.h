#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// The `sort_key` column is an array of (Float64, UInt64) tuples compared lexicographically: the first
/// tuple element carries value-based ordering, the second carries discriminators needing exact integers.
ASTPtr makeValueSortKeyComponent(ASTPtr value);
ASTPtr makeExactSortKeyComponent(ASTPtr value);

/// A fallback `sort_key` for a query whose vector input preserves row identity (the same `group`)
/// but has no explicit sort order: a stable tiebreak hashed from that group, as `or` uses per side.
ASTPtr makeFallbackSortKey(ASTPtr group_ast);

}
