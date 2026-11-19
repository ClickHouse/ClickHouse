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

/// Aggregate replication delay info across the `ReplicatedMergeTree` tables found that are sources to `storage`.
ReplicatedTableDelay getReplicatedDelay(const StoragePtr & storage, ContextPtr context);

}
