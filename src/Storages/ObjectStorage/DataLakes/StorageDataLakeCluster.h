#pragma once
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

template <typename DataLakeMetadata>
class StorageDataLakeCluster : public IStorageCluster
{
public:
    StorageDataLakeCluster(
        const String & cluster_name_,
        ObjectStorageConnectionConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_in_table_or_function_definition,
        const ConstraintsDescription & constraints_,
        const ASTPtr & partition_by,
        ContextPtr context_,
        DataLakeStorageSettingsPtr datalake_settings_,
        bool is_table_function_ = false);

    std::string getName() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ActionsDAG * filter,
        const ContextPtr & context,
        ClusterPtr cluster,
        StorageMetadataPtr storage_metadata_snapshot) const override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    void updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

protected:
    mutable DataLakeMetadataPtr current_metadata;

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    void ensureMetadataInitialized(ContextPtr context) const;
    void updateMetadata(ContextPtr context) const;

    const String engine_name;
    const ObjectStorageConnectionConfigurationPtr configuration;
    StorageObjectStorageTableOptions table_options;
    const ObjectStoragePtr object_storage;
    const DataLakeStorageSettingsPtr datalake_settings;
    NamesAndTypesList virtual_columns;
};

}
