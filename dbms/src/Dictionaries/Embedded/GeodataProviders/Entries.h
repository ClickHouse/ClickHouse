#pragma once

#include "Types.h"
#include <string>

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

