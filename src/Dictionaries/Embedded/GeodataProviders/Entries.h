#pragma once

#include <string>
#include "Types.h"

struct RegionEntry
{
    RegionID id;
    RegionID parent_id;
    RegionType type;
    RegionDepth depth;
    RegionPopulation population;
};

struct RegionNameEntry
{
    RegionID id;
    std::string name;
};
