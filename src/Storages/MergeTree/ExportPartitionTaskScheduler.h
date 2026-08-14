#pragma once

#include <Storages/MergeTree/MergeTreePartExportManifest.h>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include <vector>

namespace DB
{

class Exception;
class StorageReplicatedMergeTree;

struct ExportReplicatedMergeTreePartitionManifest;

/// todo arthur remember to add check(lock, version) when updating stuff because maybe if we believe we have the lock, we might not actually have it
class ExportPartitionTaskScheduler
{
public:
    ExportPartitionTaskScheduler(StorageReplicatedMergeTree & storage);

    /// Returns the earliest future back-off deadline (unix seconds) among parts that were skipped
    /// this tick purely because they are still backing off, or nullopt if none. The caller can use
    /// it to wake the select task sooner than the default tick interval.
    std::optional<time_t> run();
private:
    StorageReplicatedMergeTree & storage;

    /// todo  arthur maybe it is invalid to grab the manifst here
    void handlePartExportCompletion(
        const std::string & export_key,
        const std::string & part_name,
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const MergeTreePartExportManifest::CompletionCallbackResult & result);

    void handlePartExportSuccess(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const std::filesystem::path & processing_parts_path,
        const std::filesystem::path & processed_part_path,
        const std::string & part_name,
        const std::filesystem::path & export_path,
        const zkutil::ZooKeeperPtr & zk,
        const std::vector<String> & relative_paths_in_destination_storage
    );

    void handlePartExportFailure(
        const std::string & part_name,
        const std::filesystem::path & export_path,
        const zkutil::ZooKeeperPtr & zk,
        const std::optional<Exception> & exception,
        const ExportReplicatedMergeTreePartitionManifest & manifest);

    bool tryToMovePartToProcessed(
        const std::filesystem::path & export_path,
        const std::filesystem::path & processing_parts_path,
        const std::filesystem::path & processed_part_path,
        const std::string & part_name,
        const std::vector<String> & relative_paths_in_destination_storage,
        const zkutil::ZooKeeperPtr & zk
    );

    bool areAllPartsProcessed(
        const std::filesystem::path & export_path,
        const zkutil::ZooKeeperPtr & zk
    );

    struct LocalBackoff
    {
        size_t attempts = 0;
        time_t next_retry_time = 0;
    };

    /// transaction_id -> part name -> back-off state. Keyed by transaction_id (not composite
    /// key) so a reused composite key does not inherit a prior instance's back-off. Guarded by
    /// local_backoff_mutex because run() (schedule-pool thread) reads it while part-export
    /// completion callbacks (background-executor threads) write it.
    using PartNameToBackOffMap = std::unordered_map<std::string, LocalBackoff>;
    using TransactionID = std::string;
    using LocalBackoffMap = std::unordered_map<TransactionID, PartNameToBackOffMap>;

    mutable std::mutex local_backoff_mutex;
    LocalBackoffMap local_backoff TSA_GUARDED_BY(local_backoff_mutex);

    bool shouldBackOff(
        const std::string & transaction_id,
        const std::string & part_name,
        time_t now,
        std::optional<time_t> & earliest_backoff_retry) const;

    /// Record a retryable failure for (transaction_id, part_name): grow the attempt counter and
    /// compute the next eligible time. Returns the new absolute deadline.
    time_t registerLocalBackoff(
        const std::string & transaction_id,
        const std::string & part_name,
        const ExportReplicatedMergeTreePartitionManifest & manifest);

    /// Drop any back-off state for parts of (transaction_id) once they succeed or the task ends.
    void clearLocalBackoff(const std::string & transaction_id, const std::string & part_name);

    /// Remove back-off state for tasks whose transaction_id is no longer PENDING in the published
    /// model, bounding the map to the parts of currently-active tasks.
    void pruneLocalBackoff(const ExportPartitionTaskEntriesContainer::index<ExportPartitionTaskEntryTagByTransactionId>::type & model);

public:
    /// Snapshot of the local back-off map for system.replicated_partition_exports:
    /// transaction_id -> part -> (attempts, next_retry_time). Briefly locks local_backoff_mutex;
    /// never held across ZooKeeper I/O.
    std::unordered_map<TransactionID, PartNameToBackOffMap> getLocalBackoffSnapshot() const;
};

}
