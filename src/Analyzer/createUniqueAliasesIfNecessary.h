#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

void createUniqueAliasesIfNecessary(QueryTreeNodePtr & node, const ContextPtr & context);

}
