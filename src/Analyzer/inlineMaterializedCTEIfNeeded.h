#pragma once

#include <Interpreters/Context_fwd.h>

#include <unordered_set>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct MaterializedCTE;
using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;

using ReusedMaterializedCTEs = std::unordered_set<MaterializedCTEPtr>;

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, ReusedMaterializedCTEs & reused_materialized_cte, ContextPtr context);

}
