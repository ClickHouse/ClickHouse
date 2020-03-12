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
    using Metric = size_t;
    using Key = std::string;

    const char * getName(Metric event);
    const char * getDocumentation(Metric event);
    const std::vector<std::pair<String, Int8>> & getAllPossibleValues(Metric event);

    extern std::unordered_map<String, Int8> values[];
    extern std::mutex locks[];

    Metric end();

    inline void set(Metric metric, Key key, Int8 value)
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
