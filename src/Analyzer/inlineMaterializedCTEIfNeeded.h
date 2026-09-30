#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, ContextPtr context);

}
