#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Parsers/ASTFunction.h>
#include <common/logger_useful.h>
#include <Common/ActionBlocker.h>


namespace DB
{

struct Settings;
class Context;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/** A distributed table that resides on multiple servers.
  * Uses data from the specified database and tables on each server.
  *
  * You can pass one address, not several.
  * In this case, the table can be considered remote, rather than distributed.
  */
class StorageDistributed final : public ext::shared_ptr_helper<StorageDistributed>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageDistributed>;
    friend class DistributedBlockOutputStream;
    friend class StorageDistributedDirectoryMonitor;

public:
    ~StorageDistributed() override;

    std::string getName() const override { return "Distributed"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    StoragePolicyPtr getStoragePolicy() const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    bool supportsParallelInsert() const override { return true; }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    /// Removes temporary data in local filesystem.
    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;
    void renameOnDisk(const String & new_path_to_table_data);

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const override;

    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(const AlterCommands & params, const Context & context, TableLockHolder & table_lock_holder) override;

    void startup() override;
    void shutdown() override;
    void drop() override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override;

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    size_t getShardCount() const;
    const String & getRelativeDataPath() const { return relative_data_path; }
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    std::string getClusterName() const { return cluster_name; } /// Returns empty string if tables is used by TableFunctionRemote

    /// create directory monitors for each existing subdirectory
    void createDirectoryMonitors(const std::string & disk);
    /// ensure directory monitor thread and connectoin pool creation by disk and subdirectory name
    StorageDistributedDirectoryMonitor & requireDirectoryMonitor(const std::string & disk, const std::string & name);
    /// Return list of metrics for all created monitors
    /// (note that monitors are created lazily, i.e. until at least one INSERT executed)
    std::vector<StorageDistributedDirectoryMonitor::Status> getDirectoryMonitorsStatuses() const;

    void flushClusterNodesAllData();

    ClusterPtr getCluster() const;

    static IColumn::Selector createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result);
    /// Apply the following settings:
    /// - optimize_skip_unused_shards
    /// - force_optimize_skip_unused_shards
    ClusterPtr getOptimizedCluster(const Context &, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const;
    ClusterPtr skipUnusedShards(ClusterPtr cluster, const ASTPtr & query_ptr, const StorageMetadataPtr & metadata_snapshot, const Context & context) const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    NamesAndTypesList getVirtuals() const override;

    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;

    const Context & global_context;
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

protected:
    StorageDistributed(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & remote_database_,
        const String & remote_table_,
        const String & cluster_name_,
        const Context & context_,
        const ASTPtr & sharding_key_,
        const String & storage_policy_name_,
        const String & relative_data_path_,
        bool attach_,
        ClusterPtr owned_cluster_ = {});

    StorageDistributed(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ASTPtr remote_table_function_ptr_,
        const String & cluster_name_,
        const Context & context_,
        const ASTPtr & sharding_key_,
        const String & storage_policy_name_,
        const String & relative_data_path_,
        bool attach,
        ClusterPtr owned_cluster_ = {});

    String relative_data_path;

    /// Can be empty if relative_data_path is empty. In this case, a directory for the data to be sent is not created.
    StoragePolicyPtr storage_policy;
    /// The main volume to store data.
    /// Storage policy may have several configured volumes, but second and other volumes are used for parts movement in MergeTree engine.
    /// For Distributed engine such configuration doesn't make sense and only the first (main) volume will be used to store data.
    /// Other volumes will be ignored. It's needed to allow using the same multi-volume policy both for Distributed and other engines.
    VolumePtr data_volume;

    struct ClusterNodeData
    {
        std::unique_ptr<StorageDistributedDirectoryMonitor> directory_monitor;
        ConnectionPoolPtr connection_pool;

        void flushAllData() const;
        void shutdownAndDropAllData() const;
    };
    std::unordered_map<std::string, ClusterNodeData> cluster_nodes_data;
    mutable std::mutex cluster_nodes_mutex;

};

}
