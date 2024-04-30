#pragma once

#include <map>
#include <memory>

#include <base/types.h>

#include <Core/SettingsEnums.h>

namespace DB
{

struct SelectQueryInfo;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

struct ShardCursorChanges
{
    std::map<String, String> storage_restore_map;
    std::map<String, std::optional<String>> keeper_restore_map;
};

QueryTreeNodePtr buildQueryTreeForShard(const PlannerContextPtr & planner_context, QueryTreeNodePtr query_tree_to_modify);

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context);

/// calculates changes, that will be perfomed with shard query in streaming mode.
ShardCursorChanges extractShardCursorChanges(QueryTreeNodePtr query_tree, DistributedProductMode mode);

/// narrows each given cursor to shard subtree value.
/// returns true if some table expressions were changed.
bool narrowShardCursors(QueryTreeNodePtr query_tree_to_modify, size_t shard_num);

}
