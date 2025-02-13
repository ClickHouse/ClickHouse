#pragma once
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB
{

class Context;

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

    void setClusterNameInSettings(bool cluster_name_in_settings_) { cluster_name_in_settings = cluster_name_in_settings_; }

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    /*
    In case the table was created with `object_storage_cluster` setting,
    modify the AST query object so that it uses the table function implementation
    by mapping the engine name to table function name and setting `object_storage_cluster`.
    For table like
    CREATE TABLE table ENGINE=S3(...) SETTINGS object_storage_cluster='cluster'
    coverts request
    SELECT * FROM table
    to
    SELECT * FROM s3(...) SETTINGS object_storage_cluster='cluster'
    to make distributed request over cluster 'cluster'.
    */
    void updateQueryForDistributedEngineIfNeeded(ASTPtr & query);

    const String engine_name;
    const StorageObjectStorage::ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    bool cluster_name_in_settings;
};

}
