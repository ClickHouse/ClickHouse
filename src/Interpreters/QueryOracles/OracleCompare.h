#pragma once

#include <Interpreters/QueryOracles/OracleExec.h>

#include <string>

namespace DB
{

/// The single comparison layer for oracles. Equality is same-serializer text equality: both sides
/// are TabSeparated rows produced by the identical server-side serializer, so NaN, -0.0, +/-inf,
/// NULL (\N), empty strings and integer extrema render deterministically and identically — no
/// client-side normalization is ever needed or allowed.
class OracleCompare
{
public:
    static bool equal(const Rows & a, const Rows & b) { return a == b; }

    /// The shared "first <=max_diff differing rows" symmetric-diff walk over two sorted row
    /// sequences, for mismatch diagnostics. Every mismatch message uses this; none re-implements it.
    static std::string diffSummary(const Rows & a, const Rows & b, size_t max_diff = 5);
};

}
