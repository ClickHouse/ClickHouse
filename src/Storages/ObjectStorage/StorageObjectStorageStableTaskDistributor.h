#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <chrono>
#include <optional>
#include <condition_variable>
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
        std::chrono::milliseconds wait_for_owner_ = std::chrono::milliseconds(0));

    ObjectInfoPtr getNextTask(size_t number_of_current_replica);

private:
    size_t getReplicaForFile(const String & file_path);
    ObjectInfoPtr getPreQueuedFile(size_t number_of_current_replica);
    ObjectInfoPtr getMatchingFileFromIterator(size_t number_of_current_replica);
    ObjectInfoPtr getAnyUnprocessedFile(size_t number_of_current_replica);
    ObjectInfoPtr takeQueuedFile(size_t replica);

    const std::shared_ptr<IObjectIterator> iterator;
    const bool send_over_whole_archive;

    const std::chrono::milliseconds wait_for_owner;

    std::vector<std::vector<ObjectInfoPtr>> connection_to_files;
    std::unordered_map<std::string, ObjectInfoPtr> unprocessed_files;
    std::vector<size_t> remaining_bytes;

    std::vector<std::string> ids_of_nodes;
    std::vector<bool> replica_seen;
    std::optional<std::chrono::steady_clock::time_point> stealing_allowed_since;

    std::mutex mutex;
    std::condition_variable replica_seen_cv;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

}
