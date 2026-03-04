#pragma once

#include <Storages/MergeTree/MergeTreePartExportManifest.h>
#include <filesystem>

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

    void run();
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
        const std::filesystem::path & processing_parts_path,
        const std::string & part_name,
        const std::filesystem::path & export_path,
        const zkutil::ZooKeeperPtr & zk,
        const std::optional<Exception> & exception,
        size_t max_retries);

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
};

}
