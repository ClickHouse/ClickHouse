#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/Nodes/DynamicResourceManager.h>
#include <Common/Scheduler/Nodes/IOResourceManager.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context)
{
    // TODO(serxa): combine DynamicResourceManager and IOResourceManaged to work together, because now old ResourceManager is disabled
    // const auto & config = global_context->getConfigRef();
    return std::make_shared<IOResourceManager>(global_context->getWorkloadEntityStorage());
}

}
