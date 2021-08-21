#pragma once

#include <common/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Parsers/ASTFunction.h>
#include <common/logger_useful.h>
#include <Common/ActionBlocker.h>
#include <Interpreters/Cluster.h>

#include <pcg_random.hpp>

namespace DB
{

struct Settings;
class Context;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** A distributed table that resides on multiple servers.
  * Uses data from the specified database and tables on each server.
  *
  * You can pass one address, not several.
  * In this case, the table can be considered remote, rather than distributed.
  */
class StorageDistributed final : public shared_ptr_helper<StorageDistributed>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageDistributed>;
    friend class DistributedBlockOutputStream;
    friend class StorageDistributedDirectoryMonitor;
    friend class StorageSystemDistributionQueue;

public:
    ~StorageDistributed() override;

    std::string getName() const override { return "Distributed"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    StoragePolicyPtr getStoragePolicy() const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    bool supportsParallelInsert() const override { return true; }
    std::optional<UInt64> totalBytes(const Settings &) const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    QueryPipelinePtr distributedWrite(const ASTInsertQuery & query, ContextPtr context) override;

    /// Removes temporary data in local filesystem.
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(const AlterCommands & params, ContextPtr context, TableLockHolder & table_lock_holder) override;

    void startup() override;
    void shutdown() override;
    void drop() override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override;

    ActionLock getActionLock(StorageActionBlockType type) override;

    NamesAndTypesList getVirtuals() const override;

    /// Used by InterpreterInsertQuery
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    /// Returns empty string if tables is used by TableFunctionRemote
    std::string getClusterName() const { return cluster_name; }
    ClusterPtr getCluster() const;

    /// Used by InterpreterSystemQuery
    void flushClusterNodesAllData(ContextPtr context);

    /// Used by ClusterCopier
    size_t getShardCount() const;

private:
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
        bool attach_,
        ClusterPtr owned_cluster_ = {},
        ASTPtr remote_table_function_ptr_ = {});

    StorageDistributed(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ASTPtr remote_table_function_ptr_,
        const String & cluster_name_,
        ContextPtr context_,
        const ASTPtr & sharding_key_,
        const String & storage_policy_name_,
        const String & relative_data_path_,
        const DistributedSettings & distributed_settings_,
        bool attach,
        ClusterPtr owned_cluster_ = {});

    void renameOnDisk(const String & new_path_to_table_data);

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    const String & getRelativeDataPath() const { return relative_data_path; }

    /// create directory monitors for each existing subdirectory
    void createDirectoryMonitors(const DiskPtr & disk);
    /// ensure directory monitor thread and connectoin pool creation by disk and subdirectory name
    StorageDistributedDirectoryMonitor & requireDirectoryMonitor(const DiskPtr & disk, const std::string & name, bool startup);

    /// Return list of metrics for all created monitors
    /// (note that monitors are created lazily, i.e. until at least one INSERT executed)
    ///
    /// Used by StorageSystemDistributionQueue
    std::vector<StorageDistributedDirectoryMonitor::Status> getDirectoryMonitorsStatuses() const;

    static IColumn::Selector createSelector(ClusterPtr cluster, const ColumnWithTypeAndName & result);
    /// Apply the following settings:
    /// - optimize_skip_unused_shards
    /// - force_optimize_skip_unused_shards
    ClusterPtr getOptimizedCluster(ContextPtr, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const;
    ClusterPtr
    skipUnusedShards(ClusterPtr cluster, const ASTPtr & query_ptr, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) const;

    size_t getRandomShardIndex(const Cluster::ShardsInfo & shards);

    const DistributedSettings & getDistributedSettingsRef() const { return distributed_settings; }

    void delayInsertOrThrowIfNeeded() const;

    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;

    Poco::Logger * log;

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

    ActionBlocker monitors_blocker;

    String relative_data_path;

    /// Can be empty if relative_data_path is empty. In this case, a directory for the data to be sent is not created.
    StoragePolicyPtr storage_policy;
    /// The main volume to store data.
    /// Storage policy may have several configured volumes, but second and other volumes are used for parts movement in MergeTree engine.
    /// For Distributed engine such configuration doesn't make sense and only the first (main) volume will be used to store data.
    /// Other volumes will be ignored. It's needed to allow using the same multi-volume policy both for Distributed and other engines.
    VolumePtr data_volume;

    DistributedSettings distributed_settings;

    struct ClusterNodeData
    {
        std::shared_ptr<StorageDistributedDirectoryMonitor> directory_monitor;
        ConnectionPoolPtr connection_pool;
    };
    std::unordered_map<std::string, ClusterNodeData> cluster_nodes_data;
    mutable std::mutex cluster_nodes_mutex;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;
};

}
