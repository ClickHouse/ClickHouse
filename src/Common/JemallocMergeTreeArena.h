#pragma once

#include <cstddef>
#include <vector>

namespace DB::JemallocMergeTreeArena
{

/// Dedicated jemalloc arena(s) for long-lived MergeTree heap state:
///   - per-part metadata: `SerializationInfoByName`, `MergeTreeDataPartChecksums` tree,
///     the per-part `Poco::LRUCache<String, ColumnSize>(1024)` and its delegates, the
///     `ColumnSize`/`IndexSize` maps, `MinMaxIndex`, `VersionMetadataOnDisk`,
///     `index_granularity_info`, and the primary index / index-granularity arrays themselves.
///   - metadata shared across parts of a table (see `SharedPartColumns.h`): `NamesAndTypesList`,
///     `column_name_to_position`, the `serializations` map and `ColumnsSubstreams`.
///   - per-table metadata: the `MergeTreeData` object's mutable schema state — `ColumnsDescription`,
///     `VirtualColumnsDescription`, `StorageInMemoryMetadata` clones, the `serialization_hints`
///     aggregation across active parts, and the `shared_part_columns_cache` populated from
///     `setColumns`.
/// Isolating these off the default arenas reduces fragmentation of query-lifetime allocations.
///
/// Callers route allocations here for a bounded scope via `ScopedJemallocThreadArena` from
/// `Common/Jemalloc.h`. Frees auto-route via jemalloc's per-extent metadata, so only allocation
/// paths need scoping.

/// Configure the arena pool. Call once at startup, before parts are loaded. Startup-only:
/// subsequent calls are ignored.
///   num_arenas == 0 -> disabled: `getArenaIndex` returns 0 (default arena selection), a no-op.
///   num_arenas == 1 -> one shared arena.
///   num_arenas  > 1 -> a per-CPU pool. Capped at the number of CPUs the process may run on (its
///                      affinity mask); a large value (or the core count) yields one arena per
///                      allowed CPU. A dense CPU->slot map keeps every created arena reachable even
///                      under a restrictive or sparse affinity mask.
void initialize(size_t num_arenas);

/// Arena index for the calling thread's current CPU, or 0 (default arena) when disabled or not
/// yet initialized. Resolved per call from the current CPU (via the CPU->slot map) rather than
/// cached per thread, so a thread that migrates CPUs follows its CPU's arena.
unsigned getArenaIndex();

/// All arena indices in the pool (empty when disabled). For metrics aggregation and purge.
const std::vector<unsigned> & getArenaIndices();

/// Number of arenas the pool intended to create: the configured count capped at the number of
/// allowed CPUs. Compare with `getArenaIndices().size()` to detect arena-creation failures.
size_t getIntendedArenaCount();

/// Whether the pool is enabled (at least one arena created).
bool isEnabled();

/// Purge dirty pages in every pool arena, returning memory to the OS. No-op when disabled.
void purge();

}
