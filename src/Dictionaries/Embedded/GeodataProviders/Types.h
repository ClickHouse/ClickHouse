#pragma once

#include <base/types.h>

namespace DB
{

using RegionID = UInt32;
using RegionDepth = UInt8;
using RegionPopulation = UInt32;

enum class RegionType : int8_t
{
    Hidden = -1,
    Continent = 1,
    Country = 3,
    District = 4,
    Area = 5,
    City = 6,
};

}
