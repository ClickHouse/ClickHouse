#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// Convert expressions like `column = ''` or `'' = column` to `empty(column)`,
/// and `column != ''` or `'' != column` to `notEmpty(column)`
class ConvertEmptyStringComparisonToFunctionPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertEmptyStringComparisonToFunction"; }

    String getDescription() override { return "Convert comparisons to '' into empty() / notEmpty() functions."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
