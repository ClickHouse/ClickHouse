#include <IO/Resource/registerResourceManagers.h>
#include <IO/ResourceManagerFactory.h>

namespace DB
{

void registerDynamicResourceManager(ResourceManagerFactory &);

void registerResourceManagers()
{
    auto & factory = ResourceManagerFactory::instance();
    registerDynamicResourceManager(factory);
}

}
