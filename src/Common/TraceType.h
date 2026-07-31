#pragma once

#include <cstdint>

namespace DB
{

/// What a sample in `system.trace_log` records.
///
/// Kept apart from `TraceSender`, which produces these: the sender is built on POSIX signals and
/// a self-pipe and so is compiled out on Windows, but the enum names a column type that the log
/// table declares regardless of whether anything on the platform can fill it in.
enum class TraceType : uint8_t
{
    Real,
    CPU,
    Memory,
    MemorySample,
    MemoryPeak,
    ProfileEvent,
    JemallocSample,
    MemoryAllocatedWithoutCheck,
    Instrumentation
};

}
