#pragma once

#include <Common/ProfileEvents.h>

/// Defined in ProfileEvents.cpp with the list of events, used by the counter code in ProfileEventsCounters.cpp.
namespace ProfileEvents
{

/// Cells per CPU row: `Counters::num_counters` rounded up to whole cache lines.
extern const size_t per_cpu_stride;

/// `per_cpu_stride * sizeof(Count)`, so that `Counters::fetchAdd` does not scale the row offset at every per-CPU level.
extern const size_t per_cpu_stride_bytes;

/// Per-CPU storage of `global_counters`: `PerCPU::MAX_CPUS * per_cpu_stride` zero-initialized cells.
extern Count global_counters_storage[];

}
