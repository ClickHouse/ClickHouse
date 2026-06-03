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

QueryTreeNodePtr buildQueryTreeForShard(const PlannerContextPtr & planner_context, QueryTreeNodePtr query_tree_to_modify, bool allow_global_join_for_right_table);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

/// Finalize __aliasMarker nodes immediately before distributed SQL serialization:
/// materialize each marker's ColumnNode arg2 to a String constant '__tableN.col' so
/// the receiver can read it as a stable action node name. Lambda bodies are skipped
/// (marker's column there is a per-row lambda variable, not a transport-boundary
/// column). User-written markers with arbitrary arg2 pass through unchanged.
void finalizeAliasMarkersForDistributedSerialization(QueryTreeNodePtr & node, const ContextPtr & context);

}
