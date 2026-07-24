#pragma once

#include "config.h"

#include <cstdint>

namespace DB
{

/// Server-wide Silk fiber scheduler lifecycle. The scheduler is started once
/// at server startup when `disk_connections_use_silk` is on and is never
/// destroyed (scheduler threads live until process exit; destroying at
/// shutdown would require proving no fiber is parked anywhere).
/// Its fiber-switch hooks (see SilkScheduler.cpp) attach the submitter's
/// `ThreadGroup` to whichever OS thread a fiber borrows, keyed off the
/// `SilkFiberJobHeader` convention every spawned fiber's parameters follow.
/// All functions are no-ops (false) in builds without Silk.
/// This header deliberately exposes no Silk types: its consumers (e.g. the
/// server) do not link ch_contrib::silk_fibers. The scheduler options live
/// in SilkSchedulerOptions.h for the TUs that do.
void initializeSilkScheduler();

/// Set once during single-threaded server startup; safe to call concurrently
/// from any thread afterwards (atomic load).
bool isSilkSchedulerInitialized();

/// Identity of the current Silk fiber, stable across the OS threads it
/// migrates over (a fiber step suspends and resumes on whichever carrier
/// thread the scheduler picks next); 0 when not on a real fiber (plain
/// threads, proxy fibers) and in builds without Silk. Callers that need an
/// identity stable across a fiber's suspend/resume cycle (e.g. `FileSegment`'s
/// downloader tracking) must key off this instead of the OS thread id.
uint64_t currentSilkFiberId();

}
