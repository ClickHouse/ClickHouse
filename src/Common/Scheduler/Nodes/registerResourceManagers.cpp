#include <Common/Scheduler/Nodes/registerResourceManagers.h>
#include <Common/Scheduler/ResourceManagerFactory.h>

namespace DB
{

void registerDynamicResourceManager(ResourceManagerFactory &);

void registerResourceManagers()
{
    auto & factory = ResourceManagerFactory::instance();
    registerDynamicResourceManager(factory);
}

}
