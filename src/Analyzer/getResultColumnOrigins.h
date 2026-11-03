#pragma once

#include <Core/ResultColumnOrigin.h>

#include <memory>


namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

/// Returns the origin of every projection column of a resolved query tree, in the order of the projection.
/// Returns an empty vector if the query tree is not a `QueryNode` (e.g. it is a `UNION`).
ResultColumnOrigins getResultColumnOrigins(const QueryTreeNodePtr & query_tree);

}
