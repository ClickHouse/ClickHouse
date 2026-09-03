#pragma once

#include <Common/randomSeed.h>
#include <pcg_random.hpp>

inline UInt32 randomNumber()
{
    pcg64_fast rng{randomSeed()};
    std::uniform_int_distribution<pcg64_fast::result_type> dist6(
        std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return static_cast<UInt32>(dist6(rng));
}
