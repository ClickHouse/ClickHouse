#pragma once

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/ISecurityManager.h>

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
    virtual std::unique_ptr<ISecurityManager> createSecurityManager() = 0;

    virtual std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() = 0;

    // Repositories with configurations of user-defined objects (dictionaries, models)
    virtual std::unique_ptr<IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() = 0;

    virtual std::unique_ptr<IExternalLoaderConfigRepository> createExternalModelsConfigRepository() = 0;

    virtual ~IRuntimeComponentsFactory() {}
};

}
