#pragma once

#include <optional>

namespace DB::JemallocCacheArena
{

/// Enable or disable the dedicated cache arena. Must be called before any allocation.
/// When disabled, getArenaIndex() returns 0 (default arena) and purge() is a no-op.
void setEnabled(bool value);

/// Whether the dedicated cache arena is enabled.
bool isEnabled();

/// Returns the jemalloc arena index dedicated to cache allocations
/// (mark cache, uncompressed cache, etc.).
/// Creates the arena on first call (thread-safe via Meyers singleton).
/// Returns 0 (meaning "use default arena selection") if disabled or jemalloc is not available.
unsigned getArenaIndex();

/// Arena index if the arena has already been created, `std::nullopt` otherwise.
/// Unlike `getArenaIndex`, never creates the arena — for read-only inspection
/// paths (e.g. system tables) that must not mutate allocator state or throw.
std::optional<unsigned> tryGetCreatedArenaIndex();

/// Purge dirty pages only in the cache arena, returning memory to the OS.
/// No-op if disabled or jemalloc is not available.
void purge();

}
