#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <Storages/IStorage.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionActions.h>
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
class StorageDistributed : private ext::shared_ptr_helper<StorageDistributed>, public IStorage
{
    friend class ext::shared_ptr_helper<StorageDistributed>;
    friend class DistributedBlockOutputStream;
    friend class StorageDistributedDirectoryMonitor;

public:
    static StoragePtr create(
        const std::string & name_,            /// The name of the table.
        NamesAndTypesListPtr columns_,        /// List of columns.
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const String & remote_database_,    /// database on remote servers.
        const String & remote_table_,        /// The name of the table on the remote servers.
        const String & cluster_name,
        Context & context_,
        const ASTPtr & sharding_key_,
        const String & data_path_);

    static StoragePtr create(
        const std::string & name_,            /// The name of the table.
        NamesAndTypesListPtr columns_,        /// List of columns.
        const String & remote_database_,      /// database on remote servers.
        const String & remote_table_,         /// The name of the table on the remote servers.
        ClusterPtr & owned_cluster_,
        Context & context_);

    std::string getName() const override { return "Distributed"; }
    std::string getTableName() const override { return name; }
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool isRemote() const override { return true; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    void drop() override {}
    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }
    /// in the sub-tables, you need to manually add and delete columns
    /// the structure of the sub-table is not checked
    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    void shutdown() override;

    void reshardPartitions(ASTPtr query, const String  & database_name,
        const Field & first_partition, const Field & last_partition,
        const WeightedZooKeeperPaths & weighted_zookeeper_paths,
        const ASTPtr & sharding_key_expr, bool do_copy, const Field & coordinator,
        const Settings & settings) override;

    /// From each replica, get a description of the corresponding local table.
    BlockInputStreams describe(const Context & context, const Settings & settings);

    const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
    size_t getShardCount() const;
    const String & getPath() const { return path; }
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }
    std::string getClusterName() const { return cluster_name; } /// Returns empty string if tables is used by TableFunctionRemote

private:
    StorageDistributed(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const String & remote_database_,
        const String & remote_table_,
        const String & cluster_name_,
        Context & context_,
        const ASTPtr & sharding_key_ = nullptr,
        const String & data_path_ = String{});

    StorageDistributed(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const String & remote_database_,
        const String & remote_table_,
        const String & cluster_name_,
        Context & context_,
        const ASTPtr & sharding_key_ = nullptr,
        const String & data_path_ = String{});


    /// create directory monitor thread by subdirectory name
    void createDirectoryMonitor(const std::string & name);
    /// create directory monitors for each existing subdirectory
    void createDirectoryMonitors();
    /// ensure directory monitor creation
    void requireDirectoryMonitor(const std::string & name);

    ClusterPtr getCluster() const;

    /// Get monotonically increasing string to name files with data to be written to remote servers.
    String getMonotonicFileName();


    String name;
    NamesAndTypesListPtr columns;
    String remote_database;
    String remote_table;

    Context & context;
    Logger * log = &Logger::get("StorageDistributed");

    /// Used to implement TableFunctionRemote.
    std::shared_ptr<Cluster> owned_cluster;

    /// Is empty if this storage implements TableFunctionRemote.
    const String cluster_name;

    bool has_sharding_key;
    ExpressionActionsPtr sharding_key_expr;
    String sharding_key_column_name;
    String path;    /// Can be empty if data_path_ is empty. In this case, a directory for the data to be sent is not created.

    std::unordered_map<std::string, std::unique_ptr<StorageDistributedDirectoryMonitor>> directory_monitors;

    /// Used for global monotonic ordering of files to send.
    SimpleIncrement file_names_increment;
};

}
