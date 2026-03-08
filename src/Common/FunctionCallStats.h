#pragma once

#include <base/types.h>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// Global per-function execution statistics.
/// Thread-safe — uses a mutex to protect the map.
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

private:
    mutable std::mutex mutex;
    std::unordered_map<std::string, Stats> data;
};

}
