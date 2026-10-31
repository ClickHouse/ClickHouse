#pragma once

#include <base/types.h>

#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

class Context;

/// The server state that the number of threads of a query depends on: the free memory and the fair share
/// of CPU slots. Each value is taken once per query, on first use, so that every part of the query (all table
/// expressions, aggregations, subqueries, sets and pipeline resizes) is planned from the same values.
/// Created by `makeQueryContext` and shared by every context copied from the query context.
class QueryMaxThreadsSnapshot
{
public:
    /// See `getServerFreeMemory`.
    std::optional<Int64> getFreeMemory();

    /// See `ConcurrencyControl::getFairShare`.
    size_t getCPUShare();

private:
    std::once_flag free_memory_once;
    std::optional<Int64> free_memory;

    std::once_flag cpu_share_once;
    size_t cpu_share = 0;
};

using QueryMaxThreadsSnapshotPtr = std::shared_ptr<QueryMaxThreadsSnapshot>;

/// The number of threads (and streams) to plan a query with, from the settings of `context`.
/// It is `max_threads` lowered under memory pressure (see `max_threads_min_free_memory_per_thread`)
/// and, if `max_threads` is auto, to the fair share of CPU slots of the concurrency control
/// (see `use_concurrency_control_for_max_threads`). The server state is read from the snapshot of the query
/// of `context`, so the result is the same in every part of the query with the same settings.
size_t getEffectiveMaxThreads(const Context & context);

/// `threads` lowered so that every thread has at least `min_free_per_thread` bytes of free memory (at least 1),
/// with the free memory from the snapshot of the query of `context`. 0 means no limit.
size_t limitThreadsByFreeMemory(const Context & context, size_t threads, UInt64 min_free_per_thread);

}
