#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replaces chains of OR with {i}like/match to match() with combined regex or multiSearchAny.
  *
  * For example:
  *   x LIKE '%foo%' OR x LIKE '%bar%' --> multiSearchAny(x, ['foo', 'bar'])
  *   x LIKE 'foo%' OR x LIKE '%bar' --> match(x, '(^foo)|(bar$)')
  *   x LIKE '%foo%' OR x ILIKE '%bar%' --> match(x, '(foo)|((?i)bar)')
  *   x LIKE '%foo%' OR match(x, 'bar.*') --> match(x, '(foo)|(bar.*)')
  *
  * If all patterns are simple substring searches (%substring%) with the same
  * case sensitivity, uses the faster `multiSearchAny`/`multiSearchAnyCaseInsensitiveUTF8`.
  * Otherwise, uses `match` with a combined regexp pattern using alternation.
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

    String getDescription() override { return "Replaces chains of OR with {i}like/match to match() or multiSearchAny"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
