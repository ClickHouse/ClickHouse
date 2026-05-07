#pragma once

#include <ctime>

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct ReplicatedTableDelay
{
    time_t max_absolute_delay = 0;
    bool is_replicated = false;
    bool is_readonly = false;
};

/// Traverse referential dependencies of a storage (e.g. View -> ReplicatedMergeTree)
/// and aggregate delay/readonly info across all discovered ReplicatedMergeTree tables.
ReplicatedTableDelay getReplicatedDelay(const StoragePtr & storage, ContextPtr context);

}
