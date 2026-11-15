#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

namespace DB
{

class BlobKillerThread
{
    void run();

    int64_t trigger();
    void waitRound(int64_t expected_round);

public:
    BlobKillerThread(
        std::string disk_name,
        ContextPtr context,
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_,
        std::shared_ptr<BlobKillerThread> wrapped_blob_killer_);

    void startup();
    void shutdown();
    void triggerAndWait();
    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

private:
    const std::string disk_name;
    const ClusterConfigurationPtr cluster;
    const MetadataStoragePtr metadata_storage;
    const ObjectStorageRouterPtr object_storages;
    const std::shared_ptr<BlobKillerThread> wrapped_blob_killer;
    const LoggerPtr log;

    std::atomic<bool> started{false};
    std::atomic<bool> enabled{true};
    std::atomic<int64_t> finished_rounds{0};
    std::atomic<int64_t> reschedule_interval_sec{0};
    std::atomic<int64_t> metadata_request_batch{0};
    std::atomic<int64_t> max_blobs_in_task{0};
    ThreadPool remove_tasks_pool;
    ThreadPoolCallbackRunnerLocal<bool> remove_tasks_runner;

    BackgroundSchedulePoolTaskHolder task;
};

using BlobKillerThreadPtr = std::shared_ptr<BlobKillerThread>;

}
