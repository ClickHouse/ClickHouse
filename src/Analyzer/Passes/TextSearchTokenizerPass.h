#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Adds the explicit tokenizer argument to text-search functions that do not have one, taking it from the
  * text index defined on the haystack expression. `hasAnyTokens(s, ['a b'])` on a table with
  * `INDEX idx (s) TYPE text(tokenizer = array)` becomes `hasAnyTokens(s, ['a b'], 'array')`.
  *
  * Without it the function tokenizes with its default `splitByNonAlpha` and answers a different question
  * than the index does, so the result would depend on whether the index was read.
  *
  * This runs in the analyzer, before the query plan exists, because the plan loses what the query tree
  * states unambiguously: which table a column comes from. See https://github.com/ClickHouse/ClickHouse/issues/115999
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
