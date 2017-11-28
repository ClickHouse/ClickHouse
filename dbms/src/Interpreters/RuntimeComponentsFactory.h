#pragma once

#include <Interpreters/IRuntimeComponentsFactory.h>
#include <Interpreters/ExternalLoaderConfigRepository.h>
#include <Interpreters/SecurityManager.h>

namespace DB
{

/** Default implementation of runtime components factory
  * used by native server application.
  */
class RuntimeComponentsFactory : public IRuntimeComponentsFactory
{
public:
    std::unique_ptr<ISecurityManager> createSecurityManager() override
    {
        return std::make_unique<SecurityManager>();
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
