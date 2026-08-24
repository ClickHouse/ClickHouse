#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

#include <Poco/Timestamp.h>

#include <unordered_set>
#include <unordered_map>
#include <list>
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
        std::vector<std::string> && ids_of_nodes_,
        bool send_over_whole_archive_,
        uint64_t lock_object_storage_task_distribution_ms_,
        bool iceberg_read_optimization_enabled_);

    ObjectInfoPtr getNextTask(size_t number_of_current_replica);

    /// Insert objects back to unprocessed files
    void rescheduleTasksFromReplica(size_t number_of_current_replica);

private:
    size_t getReplicaForFile(const String & file_path);
    ObjectInfoPtr getPreQueuedFile(size_t number_of_current_replica);
    ObjectInfoPtr getMatchingFileFromIterator(size_t number_of_current_replica);
    ObjectInfoPtr getAnyUnprocessedFile(size_t number_of_current_replica);

    void saveLastNodeActivity(size_t number_of_current_replica);

    String getFileIdentifier(ObjectInfoPtr file_object, bool write_to_log = false) const;

    const std::shared_ptr<IObjectIterator> iterator;
    const bool send_over_whole_archive;

    std::vector<std::vector<ObjectInfoPtr>> connection_to_files;
    std::unordered_map<std::string, std::pair<ObjectInfoPtr, size_t>> unprocessed_files;

    std::vector<std::string> ids_of_nodes;

    std::unordered_map<size_t, Poco::Timestamp> last_node_activity;
    Poco::Timestamp::TimeDiff lock_object_storage_task_distribution_us;
    std::unordered_map<size_t, std::list<ObjectInfoPtr>> replica_to_files_to_be_processed;

    std::mutex mutex;
    bool iterator_exhausted = false;
    bool iceberg_read_optimization_enabled = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

}
