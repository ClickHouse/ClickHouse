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
        const ASTPtr & order_by,
        ContextPtr context_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode_,
        std::shared_ptr<DataLake::ICatalog> catalog,
        bool if_not_exists,
        bool is_datalake_query,
        bool is_table_function_ = false,
        bool lazy_init = false);

    std::string getName() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ActionsDAG * filter,
        const ContextPtr & context,
        ClusterPtr cluster,
        StorageMetadataPtr storage_metadata_snapshot) const override;

    String getPathSample(ContextPtr context);

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;
    void setClusterNameInSettings(bool cluster_name_in_settings_) { cluster_name_in_settings = cluster_name_in_settings_; }

    String getClusterName(ContextPtr context) const override;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    std::optional<QueryPipeline> distributedWrite(
        const ASTInsertQuery & query,
        ContextPtr context) override;

    void drop() override;

    void dropInnerTableIfAny(bool sync, ContextPtr context) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    void checkTableCanBeRenamed(const StorageID & new_name) const override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    IDataLakeMetadata * getExternalMetadata(ContextPtr query_context);

    StorageMetadataPtr getInMemoryMetadataPtr(bool bypass_metadata_cache = false) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr context) override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr context) const override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr context) override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    Pipe executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context) override;

    CancellationCode killMutation(const String & mutation_id) override;

    void waitForMutation(const String & mutation_id, bool wait_for_another_mutation) override;

    void setMutationCSN(const String & mutation_id, UInt64 csn) override;

    CancellationCode killPartMoveToShard(const UUID & task_uuid) override;

    void startup() override;

    void shutdown(bool is_drop = false) override;

    void flushAndPrepareForShutdown() override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    void updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

    bool supportsDelete() const override;

    bool supportsParallelInsert() const override;

    bool prefersLargeBlocks() const override;

    bool supportsPartitionBy() const override;

    bool supportsSubcolumns() const override;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override;

    /// Things required for PREWHERE.
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;
    ColumnSizeByName getColumnSizes() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    bool isObjectStorage() const override { return true; }

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    bool isClusterSupported() const override;

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
    StorageObjectStorageConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    NamesAndTypesList virtual_columns;
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
    bool cluster_name_in_settings;

    /// non-clustered storage to fall back on pure realisation if needed
    std::shared_ptr<StorageObjectStorage> pure_storage;
};

}
