#pragma once
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class StorageObjectStorageCluster : public IStorageCluster
{
public:
    StorageObjectStorageCluster(
        const String & cluster_name_,
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_in_table_or_function_definition,
        const ConstraintsDescription & constraints_,
        const ASTPtr & partition_by,
        ContextPtr context_,
        bool is_table_function_ = false,
        std::optional<FormatSettings> format_settings_ = std::nullopt,
        std::shared_ptr<DataLake::ICatalog> catalog_ = nullptr);

    std::string getName() const override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    bool isDataLake() const override { return configuration->isDataLakeConfiguration(); }

    bool isObjectStorage() const override { return true; }

    bool supportsParallelInsert() const override;

    bool supportsDelete() const override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder, DDLGuardPtr & ddl_guard) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    Pipe executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context) override;

    void drop() override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ActionsDAG * filter,
        const ContextPtr & context,
        ClusterPtr cluster,
        StorageMetadataPtr storage_metadata_snapshot) const override;

    String getPathSample(ContextPtr context);

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    void updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    const String engine_name;
    const StorageObjectStorageConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const std::shared_ptr<DataLake::ICatalog> catalog;
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
};

}
