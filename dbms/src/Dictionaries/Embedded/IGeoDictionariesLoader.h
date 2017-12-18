#pragma once

#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Dictionaries/Embedded/RegionsNames.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <memory>


// Provides actual versions of geo dictionaries (regions hierarchies, regions names)
// Bind data structures (RegionsHierarchies, RegionsNames) with data providers
class IGeoDictionariesLoader
{
public:
    virtual std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(
        const Poco::Util::AbstractConfiguration & config) = 0;

    virtual std::unique_ptr<RegionsNames> reloadRegionsNames(
        const Poco::Util::AbstractConfiguration & config) = 0;

    virtual ~IGeoDictionariesLoader() {}
};
