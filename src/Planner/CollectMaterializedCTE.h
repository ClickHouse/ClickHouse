#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

QueryTreeNodes collectMaterializedCTEs(const QueryTreeNodePtr & node);

}
