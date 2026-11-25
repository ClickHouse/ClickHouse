#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class QueryNode;

/// The `GROUP BY` keys of `query_node`, of all grouping sets.
QueryTreeNodes getGroupByKeys(const QueryNode & query_node);

/// Returns true if `node`, in an expression computed after `GROUP BY`, is a key, read from the aggregation result.
/// Under `group_by_use_nulls` the analyzer replaces a key there with a copy converted to `Nullable`, not equal to the key.
bool isGroupByKeyAfterAggregation(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys);

}
