#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

namespace DB
{

class BlobCopierThread
{
    void run();

public:
    BlobCopierThread(
        std::string disk_name_,
        ContextPtr context,
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_);

    void startup();
    void shutdown();
    void triggerAndWait();
    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

private:
    const std::string disk_name;
    const ClusterConfigurationPtr cluster;
    const MetadataStoragePtr metadata_storage;
    const ObjectStorageRouterPtr object_storages;
    const LoggerPtr log;

    std::atomic<bool> started{false};
    std::atomic<bool> enabled{false};
    std::atomic<int64_t> finished_rounds{0};
    std::atomic<int64_t> reschedule_interval_sec{0};
    std::atomic<int64_t> metadata_request_batch{0};
    ThreadPool replication_tasks_pool;
    ThreadPoolCallbackRunnerLocal<bool> replication_tasks_runner;

    BackgroundSchedulePoolTaskHolder task;
};

using BlobCopierThreadPtr = std::shared_ptr<BlobCopierThread>;

}
