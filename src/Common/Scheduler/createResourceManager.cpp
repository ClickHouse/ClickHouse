#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/WorkloadResourceManager.h>
#include <Interpreters/Context.h>

#include <memory>


namespace DB
{

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context)
{
    return std::make_shared<WorkloadResourceManager>(global_context->getWorkloadEntityStoragePtr());
}

}
