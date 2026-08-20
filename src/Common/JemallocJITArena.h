#pragma once

namespace DB::JemallocJITArena
{

/// Returns the jemalloc arena index dedicated to LLVM/JIT allocations.
/// Creates the arena on first call (thread-safe via Meyers singleton).
/// Returns 0 (meaning "use default arena selection") if jemalloc is not available, or if
/// `mallctl("arenas.create", ...)` failed at first call — in which case an error is logged
/// and `isEnabled` returns false. Passing 0 to `ScopedJemallocThreadArena` is a documented
/// no-op, so callers do not need to branch on availability.
///
/// LLVM allocates via global `operator new`, so we cannot route its allocations
/// through a custom allocator template parameter the way we do for `JemallocCacheAllocator`.
/// Instead, callers temporarily switch the calling thread's preferred arena (via
/// `mallctl("thread.arena", ...)`) using `ScopedJemallocThreadArena` for the duration
/// of any block that calls into LLVM. See `ScopedJemallocThreadArena` in `Common/Jemalloc.h`.
unsigned getArenaIndex();

/// Whether the dedicated JIT arena is available (jemalloc compiled in, embedded compiler
/// enabled, and `arenas.create` succeeded on first call).
bool isEnabled();

/// Purge dirty pages only in the JIT arena, returning memory to the OS.
/// No-op if the arena is not available (`isEnabled()` returns false).
void purge();

}
