#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <common/logger_useful.h>


namespace DB
{

class Context;
class StorageDistributedDirectoryMonitor;


/** A distributed table that resides on multiple servers.
  * Uses data from the specified database and tables on each server.
  *
  * You can pass one address, not several.
  * In this case, the table can be considered remote, rather than distributed.
  */
class StorageDistributed : public ext::shared_ptr_helper<StorageDistributed>, public IStorage
{
    friend class DistributedBlockOutputStream;
    friend class StorageDistributedDirectoryMonitor;

public:
    ~StorageDistributed() override;

    static StoragePtr createWithOwnCluster(
        const std::string & table_name_,
        const ColumnsDescription & columns_,
        const String & remote_database_,       /// database on remote servers.
        const String & remote_table_,          /// The name of the table on the remote servers.
        ClusterPtr owned_cluster_,
        const Context & context_);

    static StoragePtr createWithOwnCluster(
        const std::string & table_name_,
        const ColumnsDescription & columns_,
        ASTPtr & remote_table_function_ptr_,     /// Table function ptr.
        ClusterPtr & owned_cluster_,
        const Context & context_);

    std::string getName() const override { return "Distributed"; }
    std::string getTableName() const override { return table_name; }
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context) const override;
    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context, const ClusterPtr & cluster) const;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop() override {}

    /// Removes temporary data in local filesystem.
    void truncate(const ASTPtr &, const Context &) override;

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override { table_name = new_table_name; }
    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(
        const AlterCommands & params, const String & database_name, const String & table_name,
        const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    void startup() override;
    void shutdown() override;

    String getDataPath() const override { return path; }

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    size_t getShardCount() const;
    const String & getPath() const { return path; }
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    std::string getClusterName() const { return cluster_name; } /// Returns empty string if tables is used by TableFunctionRemote

    /// create directory monitors for each existing subdirectory
    void createDirectoryMonitors();
    /// ensure directory monitor thread creation by subdirectory name
    void requireDirectoryMonitor(const std::string & name);
    /// ensure connection pool creation and return it
    ConnectionPoolPtr requireConnectionPool(const std::string & name);

    ClusterPtr getCluster() const;


    String table_name;
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
    String path;    /// Can be empty if data_path_ is empty. In this case, a directory for the data to be sent is not created.

    struct ClusterNodeData
    {
        std::unique_ptr<StorageDistributedDirectoryMonitor> directory_monitor;
        ConnectionPoolPtr conneciton_pool;

        /// Creates connection_pool if not exists.
        void requireConnectionPool(const std::string & name, const StorageDistributed & storage);
        /// Creates directory_monitor if not exists.
        void requireDirectoryMonitor(const std::string & name, StorageDistributed & storage);

        void shutdownAndDropAllData();
    };
    std::unordered_map<std::string, ClusterNodeData> cluster_nodes_data;
    std::mutex cluster_nodes_mutex;

    /// Used for global monotonic ordering of files to send.
    SimpleIncrement file_names_increment;

protected:
    StorageDistributed(
        const String & database_name,
        const String & table_name_,
        const ColumnsDescription & columns_,
        const String & remote_database_,
        const String & remote_table_,
        const String & cluster_name_,
        const Context & context_,
        const ASTPtr & sharding_key_,
        const String & data_path_,
        bool attach);

    StorageDistributed(
        const String & database_name,
        const String & table_name_,
        const ColumnsDescription & columns_,
        ASTPtr remote_table_function_ptr_,
        const String & cluster_name_,
        const Context & context_,
        const ASTPtr & sharding_key_,
        const String & data_path_,
        bool attach);

    ClusterPtr skipUnusedShards(ClusterPtr cluster, const SelectQueryInfo & query_info);
};

}
