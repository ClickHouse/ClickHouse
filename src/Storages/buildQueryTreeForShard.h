#pragma once

#include <memory>
#include <optional>
#include <unordered_map>

#include <base/types.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct SelectQueryInfo;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class Block;

QueryTreeNodePtr buildQueryTreeForShard(const PlannerContextPtr & planner_context, QueryTreeNodePtr query_tree_to_modify, bool allow_global_join_for_right_table);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

/** When a Distributed/parallel-replicas query is executed up to `WithMergeableState`, the shard's query tree has its
  * `ALIAS` columns inlined into their defining expressions. If several projection (or sort/group/...) items expand to the
  * same expression, the shard's `ActionsDAG` deduplicates them into a single output column, so the shard header can have
  * fewer columns than the header the initiator expects (which keeps the columns distinct, since on the initiator the
  * `ALIAS` columns are not inlined).
  *
  * This helper reconstructs the initiator's `expected_header` from the shard's `shard_header` by fanning out each
  * collapsed shard column back to every expected column that maps onto it. The mapping is computed exactly: for every
  * expression node in `query_tree` we compute both its identifier-based action name (matching `expected_header`) and its
  * action name after inlining `ALIAS` columns (matching `shard_header`), and use this translation to resolve which shard
  * column feeds each expected column.
  *
  * Returns the conversion `ActionsDAG` (input = `shard_header`, output = `expected_header` names), or `std::nullopt` when
  * the situation is not a recognized projection collapse, in which case the caller should fall back to its default
  * reconciliation.
  *
  * When `duplicate_to_representative` is not null it is filled with one entry per expected column that is a fan-out
  * duplicate of an earlier expected column (i.e. several expected columns map onto the same shard column): the key is
  * the duplicate column's name and the value is the name of the first (representative) expected column it duplicates.
  * This lets a distributed aggregation merge bucket by only the distinct (representative) key columns - matching the
  * shard, which deduplicated those keys before computing its two-level bucket numbers - and reconstruct the duplicate
  * key columns after merging. Without this the initiator would bucket by more key columns than the shard did, so equal
  * groups coming from different shards could land in different buckets and never merge (wrong results).
  */
std::optional<ActionsDAG> buildShardCollapseFanOut(
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    const Block & shard_header,
    const Block & expected_header,
    std::unordered_map<String, String> * duplicate_to_representative = nullptr);

}
