#pragma once

#include "config.h"

#include <cstdint>

namespace DB
{

/// Server-wide Silk fiber scheduler lifecycle. The scheduler is started once
/// at server startup when `disk_connections_use_silk` is on and is never
/// destroyed (scheduler threads live until process exit; destroying at
/// shutdown would require proving no fiber is parked anywhere).
/// Its fiber-switch hooks (see SilkScheduler.cpp) swap `DB::current_thread` between the
/// fiber's own `ThreadStatus` and whichever OS thread the fiber currently borrows, keyed
/// off the `SilkFiberJobHeader` convention every spawned fiber's parameters follow. The
/// `ThreadStatus` itself (and its `ThreadGroup` attachment) is created by the spawn site,
/// not by these hooks.
/// Probes that `io_uring` is actually usable before starting the scheduler and throws if it
/// isn't (e.g. blocked by a container's seccomp profile) - fail-close, since a scheduler that
/// silently can't submit I/O would hang every fiber-backed disk connection instead of refusing
/// to start.
/// All functions are no-ops (false) in builds without Silk.
/// This header deliberately exposes no Silk types: its consumers (e.g. the
/// server) do not link ch_contrib::silk_fibers.
void initializeSilkScheduler();

/// Registers the reader-executor fiber-switch hooks (the `current_thread` swap for
/// `SilkFiberCategory::FETCH` fibers) on the process-wide scheduler without starting it.
/// Called by `initializeSilkScheduler`; exposed separately for unit tests, which start the
/// scheduler through the shared lazy test helper instead. No-op in builds without Silk.
void registerReaderExecutorFiberHooks();

/// Set once during single-threaded server startup; safe to call concurrently
/// from any thread afterwards (atomic load).
bool isSilkSchedulerInitialized();

/// True when a config reload has observed `disk_connections_use_silk` turned on while the
/// scheduler was never started at boot (`isSilkSchedulerInitialized` stays false forever in that
/// case - the scheduler is only ever started once, at startup). Distinguishes that half-state
/// from the setting being genuinely off, so callers like the `reader_executor_use_fibers` gate
/// (see ReadPipeline.cpp) can tell a user "restart required" instead of "turn the setting on".
/// Cleared when a later reload turns the setting back off. Set/cleared exclusively by the
/// config-reload path in Server.cpp; safe to read from any thread (atomic load).
bool isSilkConfiguredButNotStarted();

/// Records entry into (`value = true`) or exit from (`value = false`) the half-state above.
/// Called only by the config-reload path in Server.cpp.
void setSilkConfiguredButNotStarted(bool value);

/// Identity of the current Silk fiber, stable across the OS threads it
/// migrates over (a fiber step suspends and resumes on whichever carrier
/// thread the scheduler picks next); 0 when not on a real fiber (plain
/// threads, proxy fibers) and in builds without Silk. Callers that need an
/// identity stable across a fiber's suspend/resume cycle (e.g. `FileSegment`'s
/// downloader tracking) must key off this instead of the OS thread id.
uint64_t currentSilkFiberId();

}
