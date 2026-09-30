#pragma once

#include <memory>
#include <optional>
#include <base/types.h>
#include <Common/MemoryTracker.h>

/// Return most strict (by hard limit) system (non query-level, i.e. server/user/merges/...) memory limit
std::optional<UInt64> getMostStrictAvailableSystemMemory();

std::optional<UInt64> getCurrentQueryHardLimit();

/// Return current query tracked memory usage
Int64 getCurrentQueryMemoryUsage();

/// Create a memory tracker under the current query memory tracker.
std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery();

/// Limit number of threads based on free memory.
/// If free memory (server limit minus tracked) is less than threads * min_free_per_thread,
/// returns the number of threads that fit, but at least 1.
/// Returns max_threads unchanged if min_free_per_thread is 0 or no server memory limit is set.
size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread);
