#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/// Replaces chains of OR with `{i}like`/`match` by `multiSearchAny`/`multiMatchAny`/`match`.
///
/// For example:
///   x LIKE '%foo%' OR x LIKE '%bar%' --> multiSearchAny(x, ['foo', 'bar'])
///   x LIKE 'foo%' OR x LIKE '%bar' --> multiMatchAny(x, ['^foo', 'bar$'])  (with Vectorscan)
///   x LIKE 'foo%' OR x LIKE '%bar' --> match(x, '(^foo)|(bar$)')           (without Vectorscan)
///   x LIKE '%foo%' OR match(x, 'bar.*') --> multiMatchAny(x, ['foo', 'bar.*'])
///
/// If all patterns are simple substring searches (`%substring%`) with the same case sensitivity,
/// the rewrite uses the faster `multiSearchAny`/`multiSearchAnyCaseInsensitiveUTF8`.
/// Otherwise, it uses `multiMatchAny` (which leverages Vectorscan/Hyperscan) when ClickHouse is
/// built with Vectorscan, `allow_hyperscan` is on, and the per-pattern / total pattern lengths
/// fit within `max_hyperscan_regexp_length` / `max_hyperscan_regexp_total_length`. It falls back
/// to `match` with a combined regexp using alternation otherwise.
class ConvertFunctionOrLikeData
{
public:
    using TypeToVisit = ASTFunction;

    bool allow_hyperscan = true;
    size_t max_hyperscan_regexp_length = 0;
    size_t max_hyperscan_regexp_total_length = 0;
    bool reject_expensive_hyperscan_regexps = true;

    void visit(ASTFunction & function, ASTPtr & ast) const;
};

using ConvertFunctionOrLikeVisitor = InDepthNodeVisitor<OneTypeMatcher<ConvertFunctionOrLikeData>, true>;

}
