#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// Convert expressions like `column = ''` or `'' = column` to `empty(column)`,
/// and `column != ''` or `'' != column` to `notEmpty(column)`.
///
/// Keys and skip indexes match a query by the exact text of their expression, so rewriting `s = ''`
/// to `empty(s)` would break them. To avoid this, a comparison is left as written when its column belongs
/// to a table whose key or skip index checks that column for an empty string, for example
/// `INDEX idx if(s = '', 'abc', s) TYPE minmax`. A comparison inside a lambda counts for the arrays the lambda
/// runs over, as in `INDEX idx arrayMap(x -> if(x = '', 'e', x), arr) TYPE bloom_filter`. Other columns and other
/// tables are still rewritten.
class ConvertEmptyStringComparisonToFunctionPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertEmptyStringComparisonToFunction"; }

    String getDescription() override { return "Convert comparisons to '' into empty() / notEmpty() functions."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
