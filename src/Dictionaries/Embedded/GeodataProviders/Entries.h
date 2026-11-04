#pragma once

#include <string>
#include <Dictionaries/Embedded/GeodataProviders/Types.h>

namespace DB
{

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

}
