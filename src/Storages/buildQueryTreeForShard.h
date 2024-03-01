#pragma once

#include <memory>

namespace DB
{

struct SelectQueryInfo;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

QueryTreeNodePtr buildQueryTreeForShard(const PlannerContextPtr & planner_context, QueryTreeNodePtr query_tree_to_modify);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

}
