#pragma once
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <unordered_map>
#include <mutex>

namespace DB
{

class Context;

/// Simple cache to map file paths to node IDs for consistent file-to-node assignment
class FileToNodeCache
{
public:
    /// Get the node ID for a file path, or assign a new one if not present
    UInt32 getNodeForFile(const String & file_path, UInt32 total_nodes);

    /// Clear the cache
    void clear();

private:
    std::mutex mutex;
    std::unordered_map<String, UInt32> file_to_node_map;
};

class StorageObjectStorageCluster : public IStorageCluster
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StorageObjectStorageCluster(
        const String & cluster_name_,
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    String getPathSample(StorageInMemoryMetadata metadata, ContextPtr context);

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    const String engine_name;
    const StorageObjectStorage::ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    NamesAndTypesList virtual_columns;
    
    /// Cache for consistent file-to-node assignment
    mutable FileToNodeCache file_node_cache;
};

}
