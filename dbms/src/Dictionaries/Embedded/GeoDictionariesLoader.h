#pragma once

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>


// Default implementation of geo dictionaries loader used by native server application
class GeoDictionariesLoader : public IGeoDictionariesLoader
{
public:
    std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(
        const Poco::Util::AbstractConfiguration & config) override;

    std::unique_ptr<RegionsNames> reloadRegionsNames(
        const Poco::Util::AbstractConfiguration & config) override;
};
