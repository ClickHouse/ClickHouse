#pragma once

#include <Storages/IStorage.h>
#include <Storages/IStorageCluster.h>
#include <Storages/Distributed/DistributedAsyncInsertDirectoryQueue.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Common/SettingsChanges.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Common/ActionBlocker.h>
#include <Interpreters/Cluster.h>

#include <pcg_random.hpp>

namespace DB
{

struct DistributedSettings;
struct Settings;
class Context;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

/** A distributed table that resides on multiple servers.
  * Uses data from the specified database and tables on each server.
  *
  * You can pass one address, not several.
  * In this case, the table can be considered remote, rather than distributed.
  */
class StorageDistributed final : public IStorage, WithContext
{
    friend class DistributedSink;
    friend class DistributedAsyncInsertBatch;
    friend class DistributedAsyncInsertDirectoryQueue;
    friend class StorageSystemDistributionQueue;

public:
    StorageDistributed(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & remote_database_,
        const String & remote_table_,
        const String & cluster_name_,
        ContextPtr context_,
        const ASTPtr & sharding_key_,
        const String & storage_policy_name_,
        const String & relative_data_path_,
        const DistributedSettings & distributed_settings_,
        LoadingStrictnessLevel mode,
        ClusterPtr owned_cluster_ = {},
        ASTPtr remote_table_function_ptr_ = {},
        bool is_remote_function_ = false);

    ~StorageDistributed() override;

    std::string getName() const override { return "Distributed"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumnsDeprecated() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    StoragePolicyPtr getStoragePolicy() const override;

    /// Do not apply moving to PREWHERE optimization for distributed tables,
    /// because we can't be sure that underlying table supports PREWHERE.
    bool canMoveConditionsToPrewhere() const override { return false; }

    bool isRemote() const override { return true; }

    /// Snapshot for StorageDistributed contains descriptions
    /// of columns of type Object for each shard at the moment
    /// of the start of query.
    struct SnapshotData : public StorageSnapshot::Data
    {
        ColumnsDescriptionByShardNum objects_by_shard;
    };

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;
    StorageSnapshotPtr getStorageSnapshotForQuery(
        const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query, ContextPtr query_context) const override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    bool supportsParallelInsert() const override { return true; }
    std::optional<UInt64> totalBytes(const Settings &) const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool /*async_insert*/) override;

    std::optional<QueryPipeline> distributedWrite(const ASTInsertQuery & query, ContextPtr context) override;

    /// Removes temporary data in local filesystem.
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    void initializeFromDisk();
    void shutdown(bool is_drop) override;
    void flushAndPrepareForShutdown() override;
    void drop() override;

    bool storesDataOnDisk() const override { return data_volume != nullptr; }
    Strings getDataPaths() const override;

    ActionLock getActionLock(StorageActionBlockType type) override;

    /// Used by InterpreterInsertQuery
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    ClusterPtr getCluster() const;

    /// Used by InterpreterSystemQuery
    void flushClusterNodesAllData(ContextPtr context, const SettingsChanges & settings_changes);

    size_t getShardCount() const;

    bool initializeDiskOnConfigChange(const std::set<String> & new_added_disks) override;

private:
    void renameOnDisk(const String & new_path_to_table_data);

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    const String & getRelativeDataPath() const { return relative_data_path; }

    /// @param flush - if true the do flush (DistributedAsyncInsertDirectoryQueue::flushAllData()),
    /// otherwise only shutdown (DistributedAsyncInsertDirectoryQueue::shutdownWithoutFlush())
    void flushClusterNodesAllDataImpl(ContextPtr context, const SettingsChanges & settings_changes, bool flush);

    /// create directory monitors for each existing subdirectory
    void initializeDirectoryQueuesForDisk(const DiskPtr & disk);

    /// Get directory queue thread and connection pool created by disk and subdirectory name
    ///
    /// Used for the INSERT into Distributed in case of distributed_foreground_insert==1, from DistributedSink.
    DistributedAsyncInsertDirectoryQueue & getDirectoryQueue(const DiskPtr & disk, const std::string & name);

    /// Parse the address corresponding to the directory name of the directory queue
    Cluster::Addresses parseAddresses(const std::string & name) const;

    /// Return list of metrics for all created monitors
    /// (note that monitors are created lazily, i.e. until at least one INSERT executed)
    ///
    /// Used by StorageSystemDistributionQueue
    std::vector<DistributedAsyncInsertDirectoryQueue::Status> getDirectoryQueueStatuses() const;

    static IColumn::Selector createSelector(ClusterPtr cluster, const ColumnWithTypeAndName & result);
    /// Apply the following settings:
    /// - optimize_skip_unused_shards
    /// - force_optimize_skip_unused_shards
    ClusterPtr getOptimizedCluster(
        ContextPtr local_context,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        const TreeRewriterResultPtr & syntax_analyzer_result) const;

    ClusterPtr skipUnusedShards(
        ClusterPtr cluster,
        const SelectQueryInfo & query_info,
        const TreeRewriterResultPtr & syntax_analyzer_result,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr context) const;

    ClusterPtr skipUnusedShardsWithAnalyzer(
        ClusterPtr cluster, const SelectQueryInfo & query_info, const StorageSnapshotPtr & storage_snapshot, ContextPtr context) const;

    /// This method returns optimal query processing stage.
    ///
    /// Here is the list of stages (from the less optimal to more optimal):
    /// - WithMergeableState
    /// - WithMergeableStateAfterAggregation
    /// - WithMergeableStateAfterAggregationAndLimit
    /// - Complete
    ///
    /// Some simple queries without GROUP BY/DISTINCT can use more optimal stage.
    ///
    /// Also in case of optimize_distributed_group_by_sharding_key=1 the queries
    /// with GROUP BY/DISTINCT sharding_key can also use more optimal stage.
    /// (see also optimize_skip_unused_shards/allow_nondeterministic_optimize_skip_unused_shards)
    ///
    /// @return QueryProcessingStage or empty std::optoinal
    /// (in this case regular WithMergeableState should be used)
    std::optional<QueryProcessingStage::Enum> getOptimizedQueryProcessingStage(const SelectQueryInfo & query_info, const Settings & settings) const;
    std::optional<QueryProcessingStage::Enum> getOptimizedQueryProcessingStageAnalyzer(const SelectQueryInfo & query_info, const Settings & settings) const;

    size_t getRandomShardIndex(const Cluster::ShardsInfo & shards);
    std::string getClusterName() const { return cluster_name.empty() ? "<remote>" : cluster_name; }

    const DistributedSettings & getDistributedSettingsRef() const { return *distributed_settings; }

    void delayInsertOrThrowIfNeeded() const;

    std::optional<QueryPipeline> distributedWriteFromClusterStorage(const IStorageCluster & src_storage_cluster, const ASTInsertQuery & query, ContextPtr context) const;
    std::optional<QueryPipeline> distributedWriteBetweenDistributedTables(const StorageDistributed & src_distributed, const ASTInsertQuery & query, ContextPtr context) const;

    static VirtualColumnsDescription createVirtuals();

    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;
    StorageID remote_storage;

    LoggerPtr log;

    /// Used to implement TableFunctionRemote.
    std::shared_ptr<Cluster> owned_cluster;

    /// Is empty if this storage implements TableFunctionRemote.
    const String cluster_name;

    bool has_sharding_key;
    bool sharding_key_is_deterministic = false;
    ExpressionActionsPtr sharding_key_expr;
    String sharding_key_column_name;

    /// Used for global monotonic ordering of files to send.
    SimpleIncrement file_names_increment;

    ActionBlocker async_insert_blocker;

    String relative_data_path;

    /// Can be empty if relative_data_path is empty. In this case, a directory for the data to be sent is not created.
    StoragePolicyPtr storage_policy;
    /// The main volume to store data.
    /// Storage policy may have several configured volumes, but second and other volumes are used for parts movement in MergeTree engine.
    /// For Distributed engine such configuration doesn't make sense and only the first (main) volume will be used to store data.
    /// Other volumes will be ignored. It's needed to allow using the same multi-volume policy both for Distributed and other engines.
    VolumePtr data_volume;

    std::unique_ptr<DistributedSettings> distributed_settings;

    struct ClusterNodeData
    {
        std::shared_ptr<DistributedAsyncInsertDirectoryQueue> directory_queue;
        ConnectionPoolWithFailoverPtr connection_pool;
        Cluster::Addresses addresses;
        size_t clusters_version;
    };
    std::unordered_map<std::string, ClusterNodeData> cluster_nodes_data;
    mutable std::mutex cluster_nodes_mutex;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;

    bool is_remote_function;
};

}
