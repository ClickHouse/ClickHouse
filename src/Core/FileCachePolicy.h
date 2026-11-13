#pragma once

#include <cstdint>

namespace DB
{

/// Eviction policy of the filesystem cache.
enum class FileCachePolicy : uint8_t
{
    LRU,
    SLRU,
    SLRU_OVERCOMMIT,
    LRU_OVERCOMMIT,
};

}
