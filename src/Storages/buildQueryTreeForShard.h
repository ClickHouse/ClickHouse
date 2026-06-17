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

/// Inline ALIAS columns into their expressions and disambiguate projection items that only become
/// structurally identical after inlining (e.g. two ALIAS columns expanding to the same expression),
/// wrapping duplicates in __actionName(expr, '<unique>') so the shard/replica emits as many distinct
/// output columns as the initiator planner expects. Without this the duplicate columns collapse by
/// name and the position-based column match throws NUMBER_OF_COLUMNS_DOESNT_MATCH. GROUP BY / ORDER BY
/// / HAVING keys referencing a wrapped alias are rewritten to the same wrapper too, so duplicate keys
/// do not collapse at the WithMergeableState the parallel replicas execute up to. Used by both the
/// Distributed/remote() path and the parallel-replicas path before buildQueryTreeForShard.
void inlineAndDisambiguateAliasColumns(QueryTreeNodePtr & query_tree_to_modify, const ContextPtr & context);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

}
