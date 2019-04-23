#pragma once

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/IUsersManager.h>

#include <memory>

namespace DB
{

/** Factory of query engine runtime components / services.
  * Helps to host query engine in external applications
  * by replacing or reconfiguring its components.
  */
class IRuntimeComponentsFactory
{
public:
    virtual ~IRuntimeComponentsFactory() = default;

    virtual std::unique_ptr<IUsersManager> createUsersManager() = 0;

    virtual std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() = 0;

    // Repositories with configurations of user-defined objects (dictionaries, models)
    virtual std::unique_ptr<IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() = 0;

    virtual std::unique_ptr<IExternalLoaderConfigRepository> createExternalModelsConfigRepository() = 0;
};

}
