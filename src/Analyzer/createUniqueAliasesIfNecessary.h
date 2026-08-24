#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

namespace DB
{

void createUniqueAliasesIfNecessary(QueryTreeNodePtr & node, const ContextPtr & context);

}
