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
    using ConnectionKeyToReplicaMap = std::unordered_map<String, Int32>;
    using ReplicaToConnectionKeyMap = std::vector<String>;

    StorageObjectStorageStableTaskDistributor(
        std::shared_ptr<IObjectIterator> iterator_,
        const ClusterPtr & cluster);

    String getNextTask(Connection * connection);

private:
    void initializeConnectionMapping(const ClusterPtr & cluster);

    static Int32 getReplicaForFile(const String & file_path, Int32 total_replicas);
    String getPreQueuedFile(const String & connection_key);
    String getMatchingFileFromIterator(const String & connection_key, Int32 replica_idx);
    String getAnyUnprocessedFile(const String & connection_key);

    std::shared_ptr<IObjectIterator> iterator;
    ConnectionKeyToReplicaMap connection_key_to_replica;
    ReplicaToConnectionKeyMap replica_to_connection_key;
    Int32 total_replicas;
    
    std::mutex mutex;
    std::unordered_map<String, std::vector<String>> connection_to_files;
    std::unordered_set<String> unprocessed_files;
    bool iterator_exhausted = false;
    size_t total_files_processed = 0;
    
    LoggerPtr log = getLogger("StorageObjectStorageStableTaskDistributor");
};

} 
