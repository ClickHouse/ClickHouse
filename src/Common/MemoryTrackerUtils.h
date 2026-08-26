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

/// Tell the current query that memory it allocated is deliberately left to something that outlives it, such as
/// the data of an in-memory table. It is still settled when the query ends, but not reported as unaccounted.
void setCurrentQueryMemoryDriftExpected();

/// A tracker for memory that outlives the current query, such as data waiting in the asynchronous insert queue.
/// Allocate under it (`MemoryTrackerSwitcher`) and the bytes count against `max_memory_usage_for_user` until it
/// dies, wherever they are actually freed. Returns `nullptr` if the query has no user.
std::unique_ptr<MemoryTracker> createTrackerForMemoryOutlivingCurrentQuery();

/// Gives what `tracker` holds back to the current query, for data that turned out not to outlive it. Leaves the
/// tracker empty, so its destruction settles nothing.
void giveMemoryBackToCurrentQuery(MemoryTracker & tracker);

/// Create a memory tracker under the current query memory tracker.
std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery();

/// Limit number of threads based on free memory.
/// If free memory (server limit minus tracked) is less than threads * min_free_per_thread,
/// returns the number of threads that fit, but at least 1.
/// Returns max_threads unchanged if min_free_per_thread is 0 or no server memory limit is set.
size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread);
