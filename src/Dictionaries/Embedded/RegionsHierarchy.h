#pragma once

#include <vector>
#include <boost/noncopyable.hpp>
#include <common/types.h>
#include "GeodataProviders/IHierarchiesProvider.h"
#include <Core/Defines.h>


class IRegionsHierarchyDataProvider;

/** A class that lets you know if a region belongs to one RegionID region with another RegionID.
  * Information about the hierarchy of regions is downloaded from a text file.
  * Can on request update the data.
  */
class RegionsHierarchy : private boost::noncopyable
{
private:
    /// Relationship parent; 0, if there are no parents, the usual lookup table.
    using RegionParents = std::vector<RegionID>;
    /// type of region
    using RegionTypes = std::vector<RegionType>;
    /// depth in the tree, starting from the country (country: 1, root: 0)
    using RegionDepths = std::vector<RegionDepth>;
    /// population of the region. If more than 2 ^ 32 - 1, then it is equated to this maximum.
    using RegionPopulations = std::vector<RegionPopulation>;

    /// region -> parent region
    RegionParents parents;
    /// region -> city, including it or 0, if there is none
    RegionParents city;
    /// region -> country including it or 0, if there is none
    RegionParents country;
    /// region -> area that includes it or 0, if not
    RegionParents area;
    /// region -> district, including it or 0, if there is none
    RegionParents district;
    /// region -> the continent (the first when climbing in the hierarchy of regions), including it or 0, if there is none
    RegionParents continent;
    /// region -> the continent (the latter when you climb the hierarchy of regions), including it or 0, if there is none
    RegionParents top_continent;

    /// region -> population or 0, if unknown.
    RegionPopulations populations;

    /// region - depth in the tree
    RegionDepths depths;

    IRegionsHierarchyDataSourcePtr data_source;

public:
    RegionsHierarchy(IRegionsHierarchyDataSourcePtr data_source_);

    /// Reloads, if necessary, the hierarchy of regions. Not threadsafe.
    void reload();


    bool in(RegionID lhs, RegionID rhs) const
    {
        if (lhs >= parents.size())
            return false;

        for (size_t i = 0; lhs != 0 && lhs != rhs && i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
            lhs = parents[lhs];

        return lhs != 0;
    }

    RegionID toCity(RegionID region) const
    {
        if (region >= city.size())
            return 0;
        return city[region];
    }

    RegionID toCountry(RegionID region) const
    {
        if (region >= country.size())
            return 0;
        return country[region];
    }

    RegionID toArea(RegionID region) const
    {
        if (region >= area.size())
            return 0;
        return area[region];
    }

    RegionID toDistrict(RegionID region) const
    {
        if (region >= district.size())
            return 0;
        return district[region];
    }

    RegionID toContinent(RegionID region) const
    {
        if (region >= continent.size())
            return 0;
        return continent[region];
    }

    RegionID toTopContinent(RegionID region) const
    {
        if (region >= top_continent.size())
            return 0;
        return top_continent[region];
    }

    RegionID toParent(RegionID region) const
    {
        if (region >= parents.size())
            return 0;
        return parents[region];
    }

    RegionDepth getDepth(RegionID region) const
    {
        if (region >= depths.size())
            return 0;
        return depths[region];
    }

    RegionPopulation getPopulation(RegionID region) const
    {
        if (region >= populations.size())
            return 0;
        return populations[region];
    }
};
