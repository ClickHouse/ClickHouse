#include <Common/Scheduler/Workload/createWorkloadEntityStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityDiskStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityKeeperStorage.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;


namespace DB
{


namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

std::unique_ptr<IWorkloadEntityStorage> createWorkloadEntityStorage(const ContextMutablePtr & global_context)
{
    const String zookeeper_path_key = "workload_zookeeper_path";
    const String disk_path_key = "workload_path";

    const auto & config = global_context->getConfigRef();
    if (config.has(zookeeper_path_key))
    {
        if (config.has(disk_path_key))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'{}' and '{}' must not be both specified in the config",
                zookeeper_path_key,
                disk_path_key);
        }
        return std::make_unique<WorkloadEntityKeeperStorage>(global_context, config.getString(zookeeper_path_key));
    }

    String default_path = fs::path{global_context->getPath()} / "workload" / "";
    String path = config.getString(disk_path_key, default_path);
    return std::make_unique<WorkloadEntityDiskStorage>(global_context, path);
}

}
