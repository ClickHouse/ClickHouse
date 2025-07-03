#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

#include <Poco/Timestamp.h>

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>

namespace DB
{

class StorageObjectStorageStableTaskDistributor
{
public:
    StorageObjectStorageStableTaskDistributor(
        std::shared_ptr<IObjectIterator> iterator_,
        std::vector<std::string> ids_of_nodes_,
        uint64_t lock_object_storage_task_distribution_ms_);

    std::optional<String> getNextTask(size_t number_of_current_replica);

private:
    size_t getReplicaForFile(const String & file_path);
    std::optional<String> getPreQueuedFile(size_t number_of_current_replica);
    std::optional<String> getMatchingFileFromIterator(size_t number_of_current_replica);
    std::optional<String> getAnyUnprocessedFile(size_t number_of_current_replica);

    void saveLastNodeActivity(size_t number_of_current_replica);

    std::shared_ptr<IObjectIterator> iterator;

    std::vector<std::vector<String>> connection_to_files;
    /// Map of unprocessed files in format filename => number of prefetched replica
    std::unordered_map<String, size_t> unprocessed_files;

    std::vector<std::string> ids_of_nodes;
    std::unordered_map<size_t, Poco::Timestamp> last_node_activity;
    Poco::Timestamp::TimeDiff lock_object_storage_task_distribution_us;

    std::mutex mutex;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

}
