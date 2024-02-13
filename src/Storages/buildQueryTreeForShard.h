#pragma once

#include <memory>

namespace DB
{

struct SelectQueryInfo;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

QueryTreeNodePtr buildQueryTreeForShard(SelectQueryInfo & query_info, QueryTreeNodePtr query_tree_to_modify);

}
