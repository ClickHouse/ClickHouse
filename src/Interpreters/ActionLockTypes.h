#pragma once

#include <Common/ActionLock.h>


namespace DB
{

/// Keep action constants and their `system.tables.stopped_actions` names in one list so a new
/// entry cannot omit its display mapping. A missing name argument fails compilation;
/// the display tests check that names are non-empty and unique.
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

/// These helpers remain defined for the declaration and definition expansions in other files.
#define ACTION_LOCK_DECLARE(ID, VALUE, NAME) extern const StorageActionBlockType ID;
#define ACTION_LOCK_DEFINE(ID, VALUE, NAME) const StorageActionBlockType ID = VALUE;

namespace ActionLocks
{
ACTION_LOCK_TYPES(ACTION_LOCK_DECLARE)
}

}
