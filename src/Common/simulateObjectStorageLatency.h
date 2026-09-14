#pragma once

#include <base/types.h>

namespace DB
{

/// Test-only: parks the calling thread to simulate the latency of a slow object store.
/// Used by the `s3_slow_*_response` failpoints (a fast local S3 endpoint pretending to
/// respond slowly) and the `local_object_storage_slow_*` failpoints (slow disk access
/// behind `LocalObjectStorage`), so performance tests can measure how the rest of the
/// code behaves under realistic IO latency.
///
/// Deliberately a single non-inlined function: the injected wait then shows up in
/// wall-clock (`Real`) profiler stacks as one self-describing frame at the simulated IO
/// boundary, and its total duration is accounted in the
/// `SimulatedObjectStorageLatencyMicroseconds` profile event, so it can be separated
/// from genuine work in `system.query_log` and in performance reports.
void simulateObjectStorageLatency(UInt64 sleep_ms);

}
