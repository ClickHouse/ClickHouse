#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Parsers/ASTFunction.h>
#include <common/logger_useful.h>
#include <Common/ActionBlocker.h>


namespace DB
{

class Context;
class StorageDistributedDirectoryMonitor;

class Volume;
using VolumePtr = std::shared_ptr<Volume>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** A distributed table that resides on multiple servers.
  * Uses data from the specified database and tables on each server.
  *
  * You can pass one address, not several.
  * In this case, the table can be considered remote, rather than distributed.
  */
class StorageDistributed : public ext::shared_ptr_helper<StorageDistributed>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageDistributed>;
    friend class DistributedBlockOutputStream;
    friend class StorageDistributedDirectoryMonitor;

public:
    ~StorageDistributed() override;

    static StoragePtr createWithOwnCluster(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & remote_database_,       /// database on remote servers.
        const String & remote_table_,          /// The name of the table on the remote servers.
        ClusterPtr owned_cluster_,
        const Context & context_);

    static StoragePtr createWithOwnCluster(
            const StorageID & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr & remote_table_function_ptr_,     /// Table function ptr.
        ClusterPtr & owned_cluster_,
        const Context & context_);

    std::string getName() const override { return "Distributed"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context) const override;

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop(TableStructureWriteLockHolder &) override {}

    /// Removes temporary data in local filesystem.
    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    void rename(const String & new_path_to_table_data, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;
    void renameOnDisk(const String & new_path_to_table_data);

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) override;

    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    void startup() override;
    void shutdown() override;

    Strings getDataPaths() const override;

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    size_t getShardCount() const;
    std::pair<const std::string &, const std::string &> getPath();
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    std::string getClusterName() const { return cluster_name; } /// Returns empty string if tables is used by TableFunctionRemote

    /// create directory monitors for each existing subdirectory
    void createDirectoryMonitors(const std::string & disk);
    /// ensure directory monitor thread and connectoin pool creation by disk and subdirectory name
    void requireDirectoryMonitor(const std::string & disk, const std::string & name);

    void flushClusterNodesAllData();

    ClusterPtr getCluster() const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;

    Context global_context;
    Logger * log = &Logger::get("StorageDistributed");

    /// Used to implement TableFunctionRemote.
    std::shared_ptr<Cluster> owned_cluster;

    /// Is empty if this storage implements TableFunctionRemote.
    const String cluster_name;

    bool has_sharding_key;
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
        const String & storage_policy_,
        const String & relative_data_path_,
        bool attach_);

    StorageDistributed(
        const StorageID & id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ASTPtr remote_table_function_ptr_,
        const String & cluster_name_,
        const Context & context_,
        const ASTPtr & sharding_key_,
        const String & storage_policy_,
        const String & relative_data_path_,
        bool attach);

    ClusterPtr skipUnusedShards(ClusterPtr cluster, const SelectQueryInfo & query_info);

    void createStorage();

    String storage_policy;
    String relative_data_path;
    /// Can be empty if relative_data_path is empty. In this case, a directory for the data to be sent is not created.
    VolumePtr volume;

    struct ClusterNodeData
    {
        std::unique_ptr<StorageDistributedDirectoryMonitor> directory_monitor;
        ConnectionPoolPtr conneciton_pool;

        void flushAllData();
        void shutdownAndDropAllData();
    };
    std::unordered_map<std::string, ClusterNodeData> cluster_nodes_data;
    std::mutex cluster_nodes_mutex;

};

}
