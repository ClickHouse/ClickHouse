#pragma once

#include <cstdint>

namespace DB
{

enum class DistributedCachePoolBehaviourOnLimit
{
    WAIT,
    ALLOCATE_NEW_BYPASSING_POOL,
};

}
