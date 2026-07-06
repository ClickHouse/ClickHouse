#pragma once

#include "config.h"

namespace DB::JemallocMergeTreeArena
{

/// Returns the jemalloc arena index dedicated to long-lived MergeTree heap state.
/// Holds:
///   - per-part metadata: `NamesAndTypesList`, `SerializationInfoByName`, the `serializations`
///     map, `column_name_to_position`, `MergeTreeDataPartChecksums` tree, `ColumnsSubstreams`,
///     the per-part `Poco::LRUCache<String, ColumnSize>(1024)` and its delegates, the
///     `ColumnSize`/`IndexSize` maps, `MinMaxIndex`, `VersionMetadataOnDisk`,
///     `index_granularity_info`, and the primary index / index-granularity arrays themselves.
///   - per-table metadata: the `MergeTreeData` object's mutable schema state — `ColumnsDescription`,
///     `VirtualColumnsDescription`, `StorageInMemoryMetadata` clones, the `serialization_hints`
///     aggregation across active parts, and the `columns_descriptions_cache` populated from
///     `setColumns`.
///
/// Creates the arena on first call (thread-safe via Meyers singleton).
/// Returns 0 (meaning "use default arena selection") if jemalloc is not available, or if
/// `mallctl("arenas.create", ...)` failed at first call — in which case an error is logged
/// and `isEnabled` returns false. Passing 0 to `ScopedJemallocThreadArena` is a documented
/// no-op, so callers do not need to branch on availability.
///
/// Callers route allocations into this arena for a tightly-bounded scope by using
/// `ScopedJemallocThreadArena` from `Common/Jemalloc.h`. Frees auto-route via jemalloc's
/// per-extent metadata, so only allocation paths need scoping.
unsigned getArenaIndex();

/// Whether the dedicated MergeTree arena is available (jemalloc compiled in and
/// `arenas.create` succeeded on first call).
bool isEnabled();

/// Purge dirty pages only in the MergeTree arena, returning memory to the OS.
/// No-op if the arena is not available (`isEnabled()` returns false).
void purge();

}
