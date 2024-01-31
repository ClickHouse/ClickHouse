#include <IO/Resource/registerResourceManagers.h>
#include <IO/ResourceManagerFactory.h>

namespace DB
{

void registerDynamicResourceManager(ResourceManagerFactory &);
void registerStaticResourceManager(ResourceManagerFactory &);

void registerResourceManagers()
{
    auto & factory = ResourceManagerFactory::instance();
    registerDynamicResourceManager(factory);
    registerStaticResourceManager(factory);
}

}
