#include "StorageObjectStorageStableTaskDistributor.h"
#include <Common/SipHash.h>

namespace DB
{

StorageObjectStorageStableTaskDistributor::StorageObjectStorageStableTaskDistributor(
    std::shared_ptr<IObjectIterator> iterator_,
    const ClusterPtr & cluster)
    : iterator(std::move(iterator_))
    , iterator_exhausted(false)
    , total_files_processed(0)
{
    initializeConnectionMapping(cluster);
    
    LOG_INFO(
        log,
        "Initialized StorageObjectStorageStableTaskDistributor to distribute files across {} unique replicas",
        total_replicas
    );
}

void StorageObjectStorageStableTaskDistributor::initializeConnectionMapping(const ClusterPtr & cluster)
{
    connection_key_to_replica.clear();
    replica_to_connection_key.clear();
    
    const auto & addresses_with_failover = cluster->getShardsAddresses();
    
    for (size_t shard_idx = 0; shard_idx < addresses_with_failover.size(); ++shard_idx)
    {
        const auto & addresses = addresses_with_failover[shard_idx];
        
        for (const auto & address : addresses)
        {
            String connection_key = address.host_name + ":" + std::to_string(address.port);
            
            if (connection_key_to_replica.contains(connection_key))
                continue;
                
            Int32 replica_idx = static_cast<Int32>(replica_to_connection_key.size());
            connection_key_to_replica[connection_key] = replica_idx;
            replica_to_connection_key.push_back(connection_key);
            
            LOG_TRACE(
                log,
                "Discovered shard {} replica with connection key {} (replica_idx: {})",
                shard_idx,
                connection_key,
                replica_idx
            );
        }
    }
    
    total_replicas = static_cast<Int32>(replica_to_connection_key.size());
    
    LOG_INFO(
        log,
        "Mapping connections to {} unique replicas",
        total_replicas
    );
}

String StorageObjectStorageStableTaskDistributor::getNextTask(Connection * connection)
{
    String connection_key = "default";
    Int32 replica_idx = -1;
    
    if (connection)
    {
        connection_key = connection->getHost() + ":" + std::to_string(connection->getPort());
        auto it = connection_key_to_replica.find(connection_key);
        if (it != connection_key_to_replica.end())
        {
            replica_idx = it->second;
        }
    }

    LOG_TRACE(
        log,
        "Received a new connection ({}, replica_idx: {}) looking for a file",
        connection_key,
        replica_idx
    );
    
    // 1. Check pre-queued files first
    String file = getPreQueuedFile(connection_key);
    if (!file.empty())
        return file;
    
    // 2. Try to find a matching file from the iterator
    file = getMatchingFileFromIterator(connection_key, replica_idx);
    if (!file.empty())
        return file;

    if (!unprocessed_files.empty()) {
        // Prevent initiator from stealing jobs from other replicas 
        sleepForMilliseconds(50);
    }

    // 3. Process unprocessed files if iterator is exhausted
    return getAnyUnprocessedFile(connection_key);
}

Int32 StorageObjectStorageStableTaskDistributor::getReplicaForFile(const String & file_path, Int32 total_replicas)
{
    if (total_replicas <= 0)
        return 0;
        
    UInt64 hash_value = sipHash64(file_path);
    return static_cast<Int32>(hash_value % total_replicas);
}

String StorageObjectStorageStableTaskDistributor::getPreQueuedFile(const String & connection_key)
{
    std::lock_guard lock(mutex);
    auto & files_for_connection = connection_to_files[connection_key];
    
    // Find the first file that's still unprocessed
    while (!files_for_connection.empty())
    {
        String next_file = files_for_connection.back();
        files_for_connection.pop_back();
        
        // Skip if this file was already processed
        if (!unprocessed_files.contains(next_file))
            continue;
            
        unprocessed_files.erase(next_file);
        total_files_processed++;
        
        LOG_TRACE(
            log,
            "Assigning pre-queued file {} to connection {} (processed: {})",
            next_file,
            connection_key,
            total_files_processed
        );
        
        return next_file;
    }
    
    return "";
}

String StorageObjectStorageStableTaskDistributor::getMatchingFileFromIterator(const String & connection_key, Int32 replica_idx)
{
    while (!iterator_exhausted)
    {
        ObjectInfoPtr object_info;
        
        {
            std::lock_guard lock(mutex);
            object_info = iterator->next(0);
            
            if (!object_info)
            {
                iterator_exhausted = true;
                break;
            }
        }
        
        String file_path = object_info->getPath();
        Int32 file_replica_idx = getReplicaForFile(file_path, total_replicas);
        
        if (file_replica_idx == replica_idx)
        {
            std::lock_guard lock(mutex);
            total_files_processed++;

            LOG_TRACE(
                log,
                "Found file {} to connection {} (processed: {})",
                file_path,
                connection_key,
                total_files_processed
            );

            return file_path;
        }
        
        // Queue file for its assigned replica
        {
            std::lock_guard lock(mutex);
            
            unprocessed_files.insert(file_path);
            if (file_replica_idx < total_replicas)
            {
                String target_connection_key = replica_to_connection_key[file_replica_idx];
                connection_to_files[target_connection_key].push_back(file_path);
            }
        }
    }
    
    return "";
}

String StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(const String & connection_key)
{
    std::lock_guard lock(mutex);
    
    if (!unprocessed_files.empty())
    {
        auto it = unprocessed_files.begin();
        String next_file = *it;
        unprocessed_files.erase(it);
        total_files_processed++;
        
        LOG_TRACE(
            log,
            "Iterator exhausted. Assigning unprocessed file {} to connection {} (processed: {})",
            next_file,
            connection_key,
            total_files_processed
        );
        
        return next_file;
    }
    
    return "";
}

}
