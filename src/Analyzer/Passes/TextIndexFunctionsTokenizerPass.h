#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Takes the tokenizer from the text index definition and forwards it to the supported functions on that
  * index' expression, to make it explicit.
  *
  * Without forwarding, a text-search function uses the default `splitByNonAlpha` tokenizer and answers a
  * different question than the index does, so the result depends on whether the index was read. See issue
  * #115999.
  *
  * Runs in the analyzer because only the query tree states which table a column comes from.
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
