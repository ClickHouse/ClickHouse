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

struct IStorageObjectStorageTaskDistributor
{
    virtual ~IStorageObjectStorageTaskDistributor() = default;

    virtual ObjectInfoPtr getNextTask(size_t number_of_current_replica) = 0;
};

using StorageObjectStorageTaskDistributorPtr = std::shared_ptr<IStorageObjectStorageTaskDistributor>;

class StorageObjectStorageStableTaskDistributor : public IStorageObjectStorageTaskDistributor
{
public:
    StorageObjectStorageStableTaskDistributor(
        std::shared_ptr<IObjectIterator> iterator_,
        std::vector<std::string> && ids_of_nodes_,
        bool send_over_whole_archive_);

    ObjectInfoPtr getNextTask(size_t number_of_current_replica) override;

private:
    size_t getReplicaForFile(const String & file_path);
    ObjectInfoPtr getPreQueuedFile(size_t number_of_current_replica);
    ObjectInfoPtr getMatchingFileFromIterator(size_t number_of_current_replica);
    ObjectInfoPtr getAnyUnprocessedFile(size_t number_of_current_replica);

    const std::shared_ptr<IObjectIterator> iterator;
    const bool send_over_whole_archive;

    std::vector<std::vector<ObjectInfoPtr>> connection_to_files;
    std::unordered_map<std::string, ObjectInfoPtr> unprocessed_files;

    std::vector<std::string> ids_of_nodes;

    std::mutex mutex;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

class StorageObjectStorageBucketTaskDistributor : public IStorageObjectStorageTaskDistributor
{
public:
    explicit StorageObjectStorageBucketTaskDistributor(std::shared_ptr<IObjectIterator> iterator_);

    ObjectInfoPtr getNextTask(size_t number_of_current_replica) override;

private:
    const std::shared_ptr<IObjectIterator> iterator;

    std::unordered_map<size_t, std::queue<ObjectInfoPtr>> unprocessed_buckets;
    std::unordered_map<String, size_t> file_to_replica;

    std::mutex mutex;

    ObjectInfoPtr getAnyUnprocessedFile();
};

}
