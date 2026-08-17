#pragma once

#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
#include <Storages/System/StorageSystemReplicatedPartitionExports.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
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

    /// Returns a snapshot of every replicated partition export task tracked by this
    /// replica's in-memory mirror. No ZooKeeper traffic; safe to call from query threads.
    std::vector<ReplicatedPartitionExportInfo> getPartitionExportsInfo() const;

private:
    StorageReplicatedMergeTree & storage;

    void addTask(
        const ExportReplicatedMergeTreePartitionManifest & metadata,
        ExportReplicatedMergeTreePartitionTaskEntry::Status status,
        std::map<String, LastExceptionEntry> last_exception_per_replica,
        std::map<String, std::vector<String>> destination_file_paths_per_part,
        std::optional<ExportReplicatedMergeTreePartitionCommitInfoEntry> commit_info,
        const std::string & key,
        auto & entries_by_key
    );

    void removeStaleEntries(
        const std::unordered_set<std::string> & zk_children,
        auto & entries_by_key
    );

    std::mutex status_changes_mutex;
    std::queue<std::string> status_changes;

    /// M_task: serializes poll() and handleStatusChanges(). Each builds a private mutable copy of
    /// the current read-model, mutates it, and atomically publishes it via export_read_model.set().
    /// Held across ZooKeeper I/O; no reader takes it (readers use export_read_model.get()).
    std::mutex background_task_serialization_mutex;
};

}
