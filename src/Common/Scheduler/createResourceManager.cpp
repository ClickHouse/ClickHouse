#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/Nodes/DynamicResourceManager.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context)
{
    UNUSED(global_context);
    // TODO(serxa): combine DynamicResourceManager and IOResourceManaged to work together
    // const auto & config = global_context->getConfigRef();
    return std::make_shared<DynamicResourceManager>();
}

}
