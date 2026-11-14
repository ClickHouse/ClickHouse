#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/WorkloadResourceManager.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <memory>


namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsBool query_slot_admission_uses_priority;
}

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context)
{
    return std::make_shared<WorkloadResourceManager>(
        global_context->getWorkloadEntityStoragePtr(),
        global_context->getServerSettings()[ServerSetting::query_slot_admission_uses_priority]);
}

}
