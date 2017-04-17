#pragma once

#include <Dictionaries/Embedded/RegionsHierarchy.h>
#include <Poco/Exception.h>
#include <unordered_map>


/** Contains several hierarchies of regions, loaded from several different files.
  * Used to support several different perspectives on the ownership of regions by countries.
  * First of all, for the Crimea (Russian and Ukrainian points of view).
  */
class RegionsHierarchies
{
private:
    using Container = std::unordered_map<std::string, RegionsHierarchy>;
    Container data;

public:
    /** path_to_regions_hierarchy_file in configuration file
      * must point to the file with the hierarchy of regions "by default". It will be accessible by an empty key.
      * In addition, a number of files are searched for, the name of which (before the extension, if any) is added arbitrary _suffix.
      * Such files are loaded, and the hierarchy of regions is put on the `suffix` key.
      *
      * For example, if /opt/geo/regions_hierarchy.txt is specified,
      *  then the /opt/geo/regions_hierarchy_ua.txt file will also be loaded, if any, it will be accessible by the `ua` key.
      */
    RegionsHierarchies();
    explicit RegionsHierarchies(const std::string & path_to_regions_hierarchy_file);

    /// Has corresponding section in configuration file.
    static bool isConfigured();


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
