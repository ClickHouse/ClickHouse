#pragma once

#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/ExternalLoaderConfigRepository.h>
#include <Interpreters/IRuntimeComponentsFactory.h>
#include <Interpreters/UsersManager.h>

namespace DB
{

/** Default implementation of runtime components factory
  * used by native server application.
  */
class RuntimeComponentsFactory : public IRuntimeComponentsFactory
{
public:
    std::unique_ptr<IUsersManager> createUsersManager() override
    {
        return std::make_unique<UsersManager>();
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() override
    {
        return std::make_unique<GeoDictionariesLoader>();
    }

    std::unique_ptr<IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() override
    {
        return std::make_unique<ExternalLoaderConfigRepository>();
    }

    std::unique_ptr<IExternalLoaderConfigRepository> createExternalModelsConfigRepository() override
    {
        return std::make_unique<ExternalLoaderConfigRepository>();
    }
};

}
