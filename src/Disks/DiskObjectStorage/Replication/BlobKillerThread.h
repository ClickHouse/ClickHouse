#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

#include <Common/threadPoolCallbackRunner.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace DB
{

class BlobKillerThread
{
    void run();

    int64_t trigger();
    /// Wait until `finished_rounds` reaches `expected_round`, or until `deadline` is hit.
    /// Returns true if the expected round completed in time; false if the wait timed out.
    bool waitRound(int64_t expected_round, std::chrono::steady_clock::time_point deadline);

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
    /// Trigger a blob removal round and wait for it to complete.
    /// `deadline` bounds the total wait time (including the wrapped killer's wait).
    /// Returns true if the round completed before the deadline; false if it timed out.
    /// The default deadline `time_point::max` means "wait indefinitely", preserving
    /// pre-existing behavior for callers that do not care about bounded waits.
    bool triggerAndWait(
        std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max());
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
    /// `finished_rounds` is incremented after every completed `run`. Readers use the
    /// condition variable below (instead of `std::atomic::wait`) so that they can
    /// honour an externally supplied deadline. Writes go through the same mutex to
    /// preserve a clean happens-before relationship with predicate-based waiters.
    std::mutex finished_rounds_mutex;
    std::condition_variable finished_rounds_cv;
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
