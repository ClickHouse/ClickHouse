#pragma once

#include <base/types.h>


using RegionID = UInt32;
using RegionDepth = UInt8;
using RegionPopulation = UInt32;

enum class RegionType : Int8
{
    Hidden = -1,
    Continent = 1,
    Country = 3,
    District = 4,
    Area = 5,
    City = 6,
};
