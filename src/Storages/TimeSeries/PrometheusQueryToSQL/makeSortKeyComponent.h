#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// The `sort_key` column is an array of (Float64, UInt64) tuples compared lexicographically: the first
/// tuple element carries value-based ordering, the second carries discriminators needing exact integers.
ASTPtr makeValueSortKeyComponent(ASTPtr value);
ASTPtr makeExactSortKeyComponent(ASTPtr value);

}
