#pragma once

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <atomic>
#include <Core/Types.h>
#include <mutex>
#include <unordered_map>


namespace CurrentStatusInfo
{
    /// Metric identifier (index in array).
    using Metric = size_t;
    using Key = std::string;

    /// Get name of metric by identifier. Returns statically allocated string.
    const char * getName(Metric event);
    /// Get text description of metric by identifier. Returns statically allocated string.
    const char * getDocumentation(Metric event);
    const std::vector<std::pair<String, Int8>> & getAllPossibleValues(Metric event);

    extern std::unordered_map<String, String> values[];
    extern std::mutex locks[];

    /// Get index just after last metric identifier.
    Metric end();

    /// Set status of specified.
    inline void set(Metric metric, Key key, String value)
    {
        std::lock_guard<std::mutex> lock(locks[metric]);
        values[metric][key] = value;
    }

    inline void unset(Metric metric, Key key)
    {
        std::lock_guard<std::mutex> lock(locks[metric]);
        values[metric].erase(key);
    }
}
