#pragma once

#include <memory>
#include "RegionsHierarchies.h"
#include "RegionsNames.h"

#include <Poco/Util/AbstractConfiguration.h>

// Default implementation of geo dictionaries loader used by native server application
class GeoDictionariesLoader
{
public:
    std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(const Poco::Util::AbstractConfiguration & config);

    std::unique_ptr<RegionsNames> reloadRegionsNames(const Poco::Util::AbstractConfiguration & config);
};
