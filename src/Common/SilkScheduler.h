#pragma once

#include "config.h"

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
bool isSilkSchedulerInitialized();

}
