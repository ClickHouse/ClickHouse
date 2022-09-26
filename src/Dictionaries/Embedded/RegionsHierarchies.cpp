#include "RegionsHierarchies.h"

#include <Poco/DirectoryIterator.h>
#include <base/logger_useful.h>


RegionsHierarchies::RegionsHierarchies(IRegionsHierarchiesDataProviderPtr data_provider)
{
    Poco::Logger * log = &Poco::Logger::get("RegionsHierarchies");

    LOG_DEBUG(log, "Adding default regions hierarchy");
    data.emplace("", data_provider->getDefaultHierarchySource());

    for (const auto & name : data_provider->listCustomHierarchies())
    {
        LOG_DEBUG(log, "Adding regions hierarchy for {}", name);
        data.emplace(name, data_provider->getHierarchySource(name));
    }

    reload();
}
