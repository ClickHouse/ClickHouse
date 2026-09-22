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

/** Replace every `ALIAS` column node with its defining expression, so the expression is evaluated on the shard/replica
  * that reads the real table instead of the column being resolved there as if it were physical.
  *
  * Must be applied to any query tree that is about to be shipped, before `buildQueryTreeForShard`: that function rebuilds
  * a shipped table expression from column names and types only, which drops an `ALIAS` column's resolved expression and
  * leaves the remote side asking storage for a column it does not have (`NO_SUCH_COLUMN_IN_TABLE`).
  */
void inlineAliasColumns(QueryTreeNodePtr & query_tree_to_modify);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

/** When a Distributed/parallel-replicas query is executed up to `WithMergeableState`, the shard's query tree has its
  * `ALIAS` columns inlined into their defining expressions. The shard header can then differ from the header the
  * initiator expects (which keeps the `ALIAS` columns un-inlined) in three ways: it can have fewer columns, because the
  * shard's `ActionsDAG` deduplicates several projection (or sort/group/...) items that expand to the same expression;
  * it can carry the same columns in a different order, because a boundary without a projection step orders its columns
  * by first mention in the inlined tree; and it can be missing an `ALIAS` column altogether, because an `ALIAS` whose
  * declared type differs from its body's type inlines to `_CAST(<body>, '<Type>')`, which is not a column the shard
  * emits - it sends the raw columns the body reads.
  *
  * This helper reconstructs the initiator's `expected_header` from the shard's `shard_header`. The mapping is computed
  * exactly: for every expression node in `query_tree` we compute both its identifier-based action name (matching
  * `expected_header`) and its action name after inlining `ALIAS` columns (matching `shard_header`), and use this
  * translation to resolve which shard column feeds each expected column. A collapsed shard column is fanned back out to
  * every expected column that maps onto it. An expected column that no shard column can supply is computed from the
  * shard header by evaluating the inlined `ALIAS` body, which is done only while that body's value is a function of the
  * shard's columns alone. A body whose value depends on the evaluating server (`hostName`, `tcpPort`, `rand`, ...) is
  * declined instead, since evaluating it here would answer for the initiator rather than for the server that read the
  * row, which is what `inlineAliasColumns` above exists to avoid.
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
  *
  * `duplicate_to_representative` is written only when an `ActionsDAG` is returned; it is left empty on `std::nullopt`.
  * A caller that stores it unconditionally therefore never applies a collapse the returned plan does not perform.
  */
std::optional<ActionsDAG> buildShardCollapseFanOut(
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    const Block & shard_header,
    const Block & expected_header,
    std::unordered_map<String, String> * duplicate_to_representative = nullptr);

}
