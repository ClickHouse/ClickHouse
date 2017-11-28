#pragma once

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

    virtual ~IRuntimeComponentsFactory() {}
};

}
