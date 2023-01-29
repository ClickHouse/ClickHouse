#include <IO/Resource/registerResourceManagers.h>
#include <IO/ResourceManagerFactory.h>

namespace DB
{

void registerStaticResourceManager(ResourceManagerFactory &);

void registerResourceManagers()
{
    auto & factory = ResourceManagerFactory::instance();
    registerStaticResourceManager(factory);
}

}
