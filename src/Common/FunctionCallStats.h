#pragma once

#include <base/types.h>
#include <atomic>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// Global per-function execution statistics.
/// Disabled by default to avoid mutex contention on the hot path.
/// Thread-safe — uses a mutex to protect the map when enabled.
/// Used by system.functions_stats table for introspection.
struct FunctionCallStats
{
    struct Stats
    {
        UInt64 call_count = 0;
        UInt64 rows_processed = 0;
    };

    static FunctionCallStats & instance()
    {
        static FunctionCallStats inst;
        return inst;
    }

    void increment(const std::string & function_name, UInt64 rows)
    {
        /// Fast path: skip tracking entirely when disabled (no mutex, no allocation).
        if (!enabled.load(std::memory_order_relaxed))
            return;

        std::lock_guard lock(mutex);
        auto & stats = data[function_name];
        ++stats.call_count;
        stats.rows_processed += rows;
    }

    std::unordered_map<std::string, Stats> getAll() const
    {
        std::lock_guard lock(mutex);
        return data;
    }

    void reset()
    {
        std::lock_guard lock(mutex);
        data.clear();
    }

    void enable() { enabled.store(true, std::memory_order_relaxed); }
    void disable() { enabled.store(false, std::memory_order_relaxed); }
    bool isEnabled() const { return enabled.load(std::memory_order_relaxed); }

private:
    std::atomic<bool> enabled{false};
    mutable std::mutex mutex;
    std::unordered_map<std::string, Stats> data;
};

}
