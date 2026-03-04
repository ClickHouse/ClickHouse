#pragma once

#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
namespace DB
{

class StorageReplicatedMergeTree;
struct ExportReplicatedMergeTreePartitionManifest;

class ExportPartitionManifestUpdatingTask
{
public:
    ExportPartitionManifestUpdatingTask(StorageReplicatedMergeTree & storage);

    void poll();

    void handleStatusChanges();

    void addStatusChange(const std::string & key);

private:
    StorageReplicatedMergeTree & storage;

    void addTask(
        const ExportReplicatedMergeTreePartitionManifest & metadata,
        const std::string & key,
        auto & entries_by_key
    );

    void removeStaleEntries(
        const std::unordered_set<std::string> & zk_children,
        auto & entries_by_key
    );

    std::mutex status_changes_mutex;
    std::queue<std::string> status_changes;
};

}
