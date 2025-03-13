#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <memory>

namespace DB
{

class StorageObjectStorageStableTaskDistributor
{
public:
    StorageObjectStorageStableTaskDistributor(std::shared_ptr<IObjectIterator> iterator_);

    String getNextTask(size_t number_of_current_replica, size_t number_of_replicas);

private:
    static size_t getReplicaForFile(const String & file_path, size_t number_of_replicas);
    String getPreQueuedFile(size_t number_of_current_replica);
    String getMatchingFileFromIterator(size_t number_of_current_replica, size_t number_of_replicas);
    String getAnyUnprocessedFile(size_t number_of_current_replica);

    std::shared_ptr<IObjectIterator> iterator;

    std::unordered_map<size_t, std::vector<String>> connection_to_files;
    std::unordered_set<String> unprocessed_files;

    std::mutex mutex;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageObjectStorageStableTaskDistributor");
};

} 
