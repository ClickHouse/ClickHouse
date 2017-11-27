#pragma once

#include <Interpreters/IRuntimeComponentsFactory.h>
#include <Interpreters/SecurityManager.h>

namespace DB
{

class RuntimeComponentsFactory : public IRuntimeComponentsFactory
{
public:
    std::unique_ptr<ISecurityManager> createSecurityManager() override
    {
        return std::make_unique<SecurityManager>();
    }
};

}
