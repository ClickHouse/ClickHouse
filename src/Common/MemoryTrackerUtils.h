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

/// A tracker for data the current query may hand to something that outlives it, such as the asynchronous insert
/// queue. It sits under the query, so the query's own limits still apply while the data is being buffered, and
/// dropping it before the hand-off leaves the bytes charged to the query, as if it had never existed. Returns
/// `nullptr` if there is no query to charge.
std::unique_ptr<MemoryTracker> createTrackerForDataTheQueryMayHandOver();

/// Hands what `tracker` holds from the current query to its user, once the data is known to outlive the query.
/// The user was charged for it all along; from here on the query is not, and the bytes stop counting against it
/// when the tracker dies.
void handOverMemoryToTheUser(MemoryTracker & tracker);

/// Create a memory tracker under the current query memory tracker.
std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery();

/// Limit number of threads based on free memory.
/// If free memory (server limit minus tracked) is less than threads * min_free_per_thread,
/// returns the number of threads that fit, but at least 1.
/// Returns max_threads unchanged if min_free_per_thread is 0 or no server memory limit is set.
size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread);
