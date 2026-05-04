#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replaces chains of OR with `{i}like`/`match` by `multiSearchAny`/`multiMatchAny`/`match`.
  *
  * For example:
  *   x LIKE '%foo%' OR x LIKE '%bar%' --> multiSearchAny(x, ['foo', 'bar'])
  *   x LIKE 'foo%' OR x LIKE '%bar' --> multiMatchAny(x, ['^foo', 'bar$'])  (with allow_hyperscan = 1)
  *   x LIKE 'foo%' OR x LIKE '%bar' --> match(x, '(^foo)|(bar$)')           (with allow_hyperscan = 0)
  *   x LIKE '%foo%' OR match(x, 'bar.*') --> multiMatchAny(x, ['foo', 'bar.*'])
  *
  * If all patterns are simple substring searches (`%substring%`) with the same case sensitivity,
  * the rewrite uses the faster `multiSearchAny`/`multiSearchAnyCaseInsensitiveUTF8`.
  * Otherwise, it uses `multiMatchAny` (which can leverage Vectorscan/Hyperscan) when
  * `allow_hyperscan = 1` and Vectorscan is available, and falls back to `match` with a combined
  * regexp using alternation when `allow_hyperscan = 0` or Vectorscan is not compiled in.
  *
  * For pure `{i}like`/`match` OR chains, the result is wrapped with `indexHint` to preserve
  * index analysis:
  *   optimized_expr AND indexHint(original_expr)
  *
  * For mixed OR chains (`{i}like`/`match` combined with non-LIKE branches), the rewrite
  * intentionally skips the `indexHint` wrapping. Wrapping `indexHint(LIKE_subset)` would
  * prune ranges that satisfy only the non-LIKE branch, producing false negatives.
  */
class ConvertOrLikeChainPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertOrLikeChain"; }

    String getDescription() override { return "Replaces chains of OR with `{i}like`/`match` by `multiSearchAny`/`multiMatchAny`/`match`"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
