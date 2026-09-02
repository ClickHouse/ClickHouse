#pragma once

#include <memory>
#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Dictionaries/Embedded/RegionsNames.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

// Default implementation of geo dictionaries loader used by native server application
class GeoDictionariesLoader
{
public:
    static std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(const Poco::Util::AbstractConfiguration & config);
    static std::unique_ptr<RegionsNames> reloadRegionsNames(const Poco::Util::AbstractConfiguration & config);
};

}
