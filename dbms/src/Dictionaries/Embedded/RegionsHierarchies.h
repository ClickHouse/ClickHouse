#pragma once

#include <Dictionaries/Embedded/RegionsHierarchy.h>
#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>

#include <Poco/Exception.h>

#include <unordered_map>


/** Contains several hierarchies of regions.
  * Used to support several different perspectives on the ownership of regions by countries.
  * First of all, for the Crimea (Russian and Ukrainian points of view).
  */
class RegionsHierarchies
{
private:
    using Container = std::unordered_map<std::string, RegionsHierarchy>;
    Container data;

public:
    RegionsHierarchies(IRegionsHierarchiesDataProviderPtr data_provider);

    /** Reloads, if necessary, all hierarchies of regions.
      */
    void reload()
    {
        for (auto & elem : data)
            elem.second.reload();
    }


    const RegionsHierarchy & get(const std::string & key) const
    {
        auto it = data.find(key);

        if (data.end() == it)
            throw Poco::Exception("There is no regions hierarchy for key " + key);

        return it->second;
    }
};
