#pragma once

#include <Parsers/SyncReplicaMode.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

#include <unordered_set>


namespace DB
{

/// Waits until the replication queue of the table is processed. If the table is a materialized view, uses its target table.
/// Returns false if the table is not replicated.
bool trySyncReplica(StoragePtr table, SyncReplicaMode sync_replica_mode, const std::unordered_set<String> & src_replicas, ContextPtr context_);

}
