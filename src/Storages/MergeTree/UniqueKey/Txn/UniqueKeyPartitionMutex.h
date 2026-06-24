#pragma once

#include <base/types.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// Record one UNIQUE KEY mutex hold-time sample (microseconds): increments the
/// `UniqueKeyMutexHoldMicroseconds` sum by `hold_us`. Implemented in the .cpp
/// so callers don't pull in the ProfileEvents header here.
void recordUniqueKeyMutexHold(std::uint64_t hold_us);


/// UNIQUE KEY — per-partition mutex registry.
///
/// INSERT / DELETE / merge-commit paths on a UNIQUE KEY table serialize by
/// partition: one writer at a time per partition_id, but multiple
/// partitions proceed independently. This registry owns the map of
/// partition_id → mutex, guarded by a single map-level mutex for
/// lookup/insert.
///
/// Semantics:
///   - `getOrCreate(partition_id)` returns a `shared_ptr<std::mutex>` that
///     is stable for the table's lifetime — repeated calls for the same
///     partition_id return the same mutex object.
///   - Different partitions get different mutex objects.
///   - Lazy allocation: partitions are added on first use. No cleanup
///     (partition cardinality is bounded by table design).
///
/// Why `shared_ptr<std::mutex>` rather than `unique_ptr`: callers hold the
/// returned pointer across the critical section (`lock_guard<>(*m)`). Using
/// shared_ptr keeps the mutex alive even if eviction is ever added, and
/// matches the ClickHouse idiom for similar registries (see
/// `PatchJoinCache::getOrCreatePatchStats`).
///
/// This class is MergeTree-only. SharedMergeTree has no equivalent — it
/// serializes via optimistic CAS in Keeper, not a process-local mutex.
class UniqueKeyPartitionMutex
{
public:
    /// Returns the mutex associated with `partition_id`. Safe for concurrent
    /// calls. Creates the mutex on first use for a given partition.
    std::shared_ptr<std::mutex> getOrCreate(const String & partition_id);

private:
    mutable std::mutex map_mutex;
    std::unordered_map<String, std::shared_ptr<std::mutex>> partition_mutexes;
};

}
