#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Takes the tokenizer from the text index on the haystack and makes it explicit, so that
  * `hasAnyTokens(s, ['a b'])` on `INDEX idx (s) TYPE text(tokenizer = array)` becomes
  * `hasAnyTokens(s, ['a b'], 'array')`. Otherwise the function answers with its default
  * `splitByNonAlpha` and the result depends on whether the index was read (issue #115999).
  *
  * Runs in the analyzer because only the query tree states which table a column comes from.
  */
class TextSearchTokenizerPass final : public IQueryTreePass
{
public:
    String getName() override { return "TextSearchTokenizer"; }

    String getDescription() override
    {
        return "Add the tokenizer of the text index as an explicit argument of text-search functions";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
