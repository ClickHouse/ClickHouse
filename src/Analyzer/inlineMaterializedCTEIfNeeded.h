#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

/// Returns true only when the resolved QueryTree root was replaced by a cloned
/// generation. In-place duplicate-CTE canonicalization does not change node
/// identities and therefore returns false.
bool inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, ContextPtr context);
}
