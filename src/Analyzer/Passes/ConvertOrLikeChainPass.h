#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replaces chains of OR with `{i}like`/`match` by `multiSearchAny`/`multiMatchAny`.
  *
  * For example:
  *   x LIKE '%foo%' OR x LIKE '%bar%' --> multiSearchAny(x, ['foo', 'bar'])
  *   x LIKE 'foo%' OR x LIKE '%bar' --> multiMatchAny(x, ['^foo', 'bar$'])  (with allow_hyperscan = 1)
  *   x LIKE '%foo%' OR match(x, 'bar.*') --> multiMatchAny(x, ['foo', 'bar.*'])
  *
  * If all patterns are simple substring searches (`%substring%`) with the same case sensitivity,
  * the rewrite uses the faster `multiSearchAny`/`multiSearchAnyCaseInsensitiveUTF8`. Otherwise it
  * uses `multiMatchAny` (Vectorscan/Hyperscan) when `allow_hyperscan = 1`, Vectorscan is available,
  * and the patterns are eligible. When neither fast path applies, the original `OR` chain is kept
  * unchanged: a combined `match('(p1)|(p2)|...')` alternation over RE2 is consistently slower than
  * the original short-circuit `OR`, so it is never emitted.
  *
  * For pure `{i}like`/`match` OR chains, the result is wrapped with `indexHint` to preserve
  * index analysis:
  *   optimized_expr AND indexHint(original_expr)
  *
  * For mixed OR chains (`{i}like`/`match` combined with non-LIKE branches), the rewrite
  * intentionally skips the `indexHint` wrapping. Wrapping `indexHint(LIKE_subset)` would
  * prune ranges that satisfy only the non-LIKE branch, producing false negatives.
  *
  * The two rewrite targets have per-target minimum branch counts, calibrated on `hits`:
  * `multiSearchAny` from `optimize_or_like_chain_min_substrings` branches (~4), `multiMatchAny`
  * from `optimize_or_like_chain_min_patterns` branches (~9-10). Shorter chains are kept verbatim
  * because the fixed setup cost of the rewrite outweighs short-circuit OR evaluation for few patterns.
  */
class ConvertOrLikeChainPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertOrLikeChain"; }

    String getDescription() override { return "Replaces chains of OR with `{i}like`/`match` by `multiSearchAny`/`multiMatchAny`"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
