#pragma once

#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/ExternalLoaderConfigRepository.h>
#include <Interpreters/UsersManager.h>

namespace DB
{

/** Default implementation of runtime components factory
  * used by native server application.
  */
class RuntimeComponentsFactory
{
public:
    std::unique_ptr<IUsersManager> createUsersManager()
    {
        return std::make_unique<UsersManager>();
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader()
    {
        return std::make_unique<GeoDictionariesLoader>();
    }

    std::unique_ptr<IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository()
    {
        return std::make_unique<ExternalLoaderConfigRepository>();
    }

    std::unique_ptr<IExternalLoaderConfigRepository> createExternalModelsConfigRepository()
    {
        return std::make_unique<ExternalLoaderConfigRepository>();
    }
};

}
