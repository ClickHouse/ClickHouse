#pragma once

#include <cstdint>

namespace DB
{

enum class DistributedCacheLogMode
{
    LOG_NOTHING,
    LOG_ON_ERROR,
    LOG_ALL,
};

}
