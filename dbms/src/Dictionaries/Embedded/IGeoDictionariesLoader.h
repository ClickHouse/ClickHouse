#pragma once

#include <memory>
#include "RegionsHierarchies.h"
#include "RegionsNames.h"

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}

class Logger;
}


// Provides actual versions of geo dictionaries (regions hierarchies, regions names)
// Bind data structures (RegionsHierarchies, RegionsNames) with data providers
class IGeoDictionariesLoader
{
public:
    virtual std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(const Poco::Util::AbstractConfiguration & config) = 0;

    virtual std::unique_ptr<RegionsNames> reloadRegionsNames(const Poco::Util::AbstractConfiguration & config) = 0;

    virtual ~IGeoDictionariesLoader() {}
};
