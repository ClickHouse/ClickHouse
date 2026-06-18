#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Coalesce chains of `hasPhrase` over the same column into a single `hasAnyPhrases`/`hasAllPhrases` call,
  * so the column is tokenized once instead of once per phrase:
  *
  *     hasPhrase(c, 'p0') OR  hasPhrase(c, 'p1')  ->  hasAnyPhrases(c, ['p0', 'p1'])
  *     hasPhrase(c, 'p0') AND hasPhrase(c, 'p1')  ->  hasAllPhrases(c, ['p0', 'p1'])
  *
  * Only calls that share the same input column AND the same tokenizer specification are grouped.
  * A call with no explicit tokenizer argument is kept separate from one with an explicit tokenizer
  * (even an explicit `'splitByNonAlpha'`), because the former binds to the text index's tokenizer
  * while the latter is always evaluated with the named tokenizer - they are not interchangeable.
  *
  * `OR` coalescing is gated by `optimize_rewrite_has_phrase_or_chain` (on by default); `AND` coalescing
  * by `optimize_rewrite_has_phrase_and_chain` (off by default, because an `AND` chain short-circuits and
  * coalescing it can regress selective filters).
  */
class CoalesceHasPhrasePass final : public IQueryTreePass
{
public:
    String getName() override { return "CoalesceHasPhrase"; }

    String getDescription() override { return "Coalesce OR/AND chains of hasPhrase on the same column into hasAnyPhrases/hasAllPhrases"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
