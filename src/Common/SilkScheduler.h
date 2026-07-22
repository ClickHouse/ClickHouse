#pragma once

#include "config.h"

namespace DB
{

/// Server-wide Silk fiber scheduler lifecycle. The scheduler is started once
/// at server startup when `disk_connections_use_silk` is on and is never
/// destroyed (scheduler threads live until process exit; destroying at
/// shutdown would require proving no fiber is parked anywhere).
/// All functions are no-ops (false) in builds without Silk.
void initializeSilkScheduler();
bool isSilkSchedulerInitialized();

}
