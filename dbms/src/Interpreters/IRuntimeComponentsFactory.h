#pragma once

#include <Interpreters/ISecurityManager.h>

#include <memory>

namespace DB
{

class IRuntimeComponentsFactory
{
public:
    virtual std::unique_ptr<ISecurityManager> createSecurityManager() = 0;

    virtual ~IRuntimeComponentsFactory() {}
};

}
