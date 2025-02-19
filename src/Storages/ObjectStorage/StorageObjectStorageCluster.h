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
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode_,
        ASTPtr partition_by_ = nullptr
    );

    std::string getName() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    String getPathSample(StorageInMemoryMetadata metadata, ContextPtr context);

    void setClusterNameInSettings(bool cluster_name_in_settings_) { cluster_name_in_settings = cluster_name_in_settings_; }

    String getClusterName(ContextPtr context) const override;

    void setInMemoryMetadata(const StorageInMemoryMetadata & metadata_) override
    {
        if (pure_storage)
            pure_storage->setInMemoryMetadata(metadata_);
        IStorageCluster::setInMemoryMetadata(metadata_);
    }

    void setVirtuals(VirtualColumnsDescription virtuals_) override
    {
        if (pure_storage)
            pure_storage->setVirtuals(virtuals_);
        IStorageCluster::setVirtuals(virtuals_);
    }

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    void readFallBackToPure(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr writeFallBackToPure(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    std::shared_ptr<StorageObjectStorage> getPureStorage(ContextPtr context);
    
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
    void updateQueryForDistributedEngineIfNeeded(ASTPtr & query, ContextPtr context);

    const String engine_name;
    const StorageObjectStorage::ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    bool cluster_name_in_settings;

    std::mutex mutex;
    std::shared_ptr<StorageObjectStorage> pure_storage;
    String comment;
    std::optional<FormatSettings> format_settings;
    LoadingStrictnessLevel mode;
    ASTPtr partition_by;
};

}
