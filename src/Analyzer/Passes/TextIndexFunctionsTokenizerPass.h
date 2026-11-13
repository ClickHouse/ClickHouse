#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Forwards the tokenizer of a text index into the supported functions on that index' expression, so a
  * predicate the plan cannot reach still answers what the index does (issue #115999).
  *
  * Runs in the analyzer because only the query tree states which table a column comes from.
  *
  * Only the tokenizer. An index `preprocessor` or `postprocessor`, an opaque `StorageView`, a
  * `Distributed` initiator that is not a shard, an element access over a subquery output that
  * `FunctionToSubcolumnsPass` could not normalize, and whatever this pass declines still diverge
  * between scan and stranded evaluation. See `02346_text_index_bug115999`.
  */
class TextIndexFunctionsTokenizerPass final : public IQueryTreePass
{
public:
    String getName() override { return "TextIndexFunctionsTokenizer"; }

    String getDescription() override
    {
        return "Forwards tokenizer from the text index definition into supported text search functions";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
