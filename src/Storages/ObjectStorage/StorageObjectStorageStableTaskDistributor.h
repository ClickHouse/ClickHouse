#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <unordered_set>
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
        size_t number_of_replicas_);

    ObjectInfoPtr getNextTask(size_t number_of_current_replica);

private:
    size_t getReplicaForFile(const String & file_path);
    ObjectInfoPtr getPreQueuedFile(size_t number_of_current_replica);
    ObjectInfoPtr getMatchingFileFromIterator(size_t number_of_current_replica);
    ObjectInfoPtr getAnyUnprocessedFile(size_t number_of_current_replica);

    std::shared_ptr<IObjectIterator> iterator;

    std::vector<std::vector<ObjectInfoPtr>> connection_to_files;
    std::unordered_map<std::string, ObjectInfoPtr> unprocessed_files;

    std::mutex mutex;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

}
