#pragma once

#include <memory>
#include <optional>
#include <string_view>
#include <base/types.h>
#include <Common/MemoryTracker.h>

/// Return most strict (by hard limit) system (non query-level, i.e. server/user/merges/...) memory limit
std::optional<UInt64> getMostStrictAvailableSystemMemory();

/// The query memory threshold at which an operator spills to disk, from its absolute setting `max_bytes` and
/// its ratio setting `max_bytes_ratio`, where 0 disables either. The ratio applies to the memory available
/// under the strictest server or user limit (see `getMostStrictAvailableSystemMemory`) and has no effect
/// without such a limit. When both apply, the smaller threshold is used. An enabled ratio gives at least one
/// byte, because 0 disables spilling. `ratio_setting_name` names the ratio setting in the error for a ratio
/// outside [0, 1) and in the log.
size_t getMaxBytesBeforeExternalProcessing(size_t max_bytes, double max_bytes_ratio, std::string_view ratio_setting_name);

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
