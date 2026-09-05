#pragma once

#include <cstdint>

#include <base/types.h>

namespace DB
{

/// The `memory_context` of a sample that has none. Part of what `system.trace_log` stores, hence
/// here rather than on `TraceSender`, which does not exist on every platform.
static constexpr Int8 TRACE_MEMORY_CONTEXT_UNKNOWN = -1;

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
