#pragma once

#include <base/types.h>

namespace DB
{

/// Test-only slow-object-store sleep for the `s3_slow_*_response` / `local_object_storage_slow_*` failpoints; one NO_INLINE `Real`-profile frame, accounted in `SimulatedObjectStorageLatencyMicroseconds`.
void simulateObjectStorageLatency(UInt64 sleep_ms);

}
