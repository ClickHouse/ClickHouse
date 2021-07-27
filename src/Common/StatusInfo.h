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
    using Status = size_t;
    using Key = std::string;

    const char * getName(Status event);
    const char * getDocumentation(Status event);
    const std::vector<std::pair<String, Int8>> & getAllPossibleValues(Status event);

    extern std::unordered_map<String, Int8> values[];
    extern std::mutex locks[];

    Status end();

    inline void set(Status status, Key key, Int8 value)
    {
        std::lock_guard<std::mutex> lock(locks[status]);
        values[status][key] = value;
    }

    inline void unset(Status status, Key key)
    {
        std::lock_guard<std::mutex> lock(locks[status]);
        values[status].erase(key);
    }
}
