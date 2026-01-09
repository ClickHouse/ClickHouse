#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Collect list of selected columns for a specific table from query tree.
std::vector<String> collectSelectedColumnsFromTable(QueryTreeNodePtr & query_tree, const StorageID & storage_id, const ContextPtr & context);

}
