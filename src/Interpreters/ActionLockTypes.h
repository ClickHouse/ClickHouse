#pragma once

#include <Common/ActionLock.h>


namespace DB
{

/// The single source of truth for every `StorageActionBlockType` tracked by `ActionLocksManager`:
/// the constant value and the display name shown in the `stopped_actions` column of `system.tables`.
///
/// Adding a new action type here is the only step needed to keep the constants, `getActionName`,
/// and the drift unit test in sync: a missing or duplicated display name breaks the build.
/// The list must stay in the same order as the action type values.
#define ACTION_LOCK_TYPES(M) \
    M(PartsMerge,            1,  "merges")                 \
    M(PartsFetch,            2,  "fetches")                \
    M(PartsSend,             3,  "replicated_sends")       \
    M(ReplicationQueue,      4,  "replication_queue")      \
    M(DistributedSend,       5,  "distributed_sends")      \
    M(PartsTTLMerge,         6,  "ttl_merges")             \
    M(PartsMove,             7,  "moves")                  \
    M(PullReplicationLog,    8,  "pull_replication_log")   \
    M(Cleanup,               9,  "cleanup")                \
    M(ViewRefresh,           10, "view_refresh")           \
    M(VirtualPartsUpdate,    11, "virtual_parts_update")   \
    M(ReduceBlockingParts,   12, "reduce_blocking_parts")  \
    M(ViewRefreshPause,      13, "view_refresh_pause")     \
    M(StreamConsume,         14, "streaming_consumption")

/// Per-entry expansion helpers, kept defined so that other translation units can
/// generate declarations, definitions, or test expectations from `ACTION_LOCK_TYPES`.
#define ACTION_LOCK_DECLARE(ID, VALUE, NAME) extern const StorageActionBlockType ID;
#define ACTION_LOCK_DEFINE(ID, VALUE, NAME) const StorageActionBlockType ID = VALUE;

namespace ActionLocks
{
ACTION_LOCK_TYPES(ACTION_LOCK_DECLARE)
}

}
