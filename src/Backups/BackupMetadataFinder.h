#pragma once

#include <Backups/RestoreSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/TablesDependencyGraph.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/setThreadName.h>

#include <filesystem>
#include <future>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;


/// Base class for discovering and navigating backup metadata structure.
/// Provides methods to find databases, tables, and root paths in backups,
/// as well as async task management for parallel operations.
class BackupMetadataFinder : private boost::noncopyable
{
public:
    virtual ~BackupMetadataFinder() = default;

    /// Formats a table name with its type prefix for use in log messages and error messages.
    /// @param database_name The name of the database containing the table.
    ///                      If equals to DatabaseCatalog::TEMPORARY_DATABASE, the table is treated as temporary.
    /// @param table_name The name of the table.
    /// @param first_upper If true, capitalizes the first letter of the output (e.g., "Table" instead of "table").
    /// @return A formatted string:
    ///         - For temporary tables: "temporary table `<table_name>`" (or "Temporary table ..." if first_upper)
    ///         - For regular tables: "table `<database>`.`<table_name>`" (or "Table ..." if first_upper)
    static String tableNameWithTypeToString(const String & database_name, const String & table_name, bool first_upper);

protected:
    BackupMetadataFinder(
        const RestoreSettings & restore_settings_,
        const BackupPtr & backup_,
        const ContextMutablePtr & context_,
        const ContextPtr & query_context_,
        ThreadPool & thread_pool_);

    /// Finds root paths in the backup considering shard/replica configuration.
    void findRootPathsInBackup();

    /// Finds specific database in the backup.
    void findDatabaseInBackup(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names);
    void findDatabaseInBackupImpl(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names);

    /// Finds specific table in the backup.
    void
    findTableInBackup(const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions);
    void findTableInBackupImpl(
        const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions);

    /// Finds all databases and tables in the backup except specified ones.
    void findEverythingInBackup(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names);

    /// Returns the number of discovered databases and tables.
    size_t getNumDatabases() const;
    size_t getNumTables() const;

    /// Schedule a task from the thread pool and start executing it.
    virtual void schedule(std::function<void()> && task_, ThreadName thread_name_);

    /// Returns the number of currently scheduled or executing tasks.
    size_t getNumFutures() const;

    /// Waits until all tasks are processed (including the tasks scheduled while we're waiting).
    /// Throws an exception if any of the tasks throws an exception.
    void waitFutures(bool throw_if_error = true);

    /// Throws an exception if the query was cancelled.
    void checkIsQueryCancelled() const;

    /// Apply custom storage policy to a CREATE query (can be overridden by derived classes).
    virtual void applyCustomStoragePolicy(ASTPtr query_ptr);

    /// Information about a database found in the backup.
    struct DatabaseInfo
    {
        ASTPtr create_database_query;
        String create_database_query_str;
        bool is_predefined_database = false;
        DatabasePtr database;
        String metadata_path_in_backup;
    };

    /// Information about a table found in the backup.
    struct TableInfo
    {
        ASTPtr create_table_query;
        String create_table_query_str;
        bool is_predefined_table = false;
        bool has_data = false;
        std::filesystem::path data_path_in_backup;
        std::optional<ASTs> partitions;
        DatabasePtr database;
        StoragePtr storage;
        TableLockHolder table_lock;
        String metadata_path_in_backup;
    };

    const RestoreSettings restore_settings;
    BackupPtr backup;
    ContextMutablePtr context;
    ContextPtr query_context;
    QueryStatusPtr process_list_element;
    LoggerPtr log;

    DDLRenamingMap renaming_map;
    std::vector<std::filesystem::path> root_paths_in_backup;

    std::unordered_map<String, DatabaseInfo> database_infos TSA_GUARDED_BY(mutex);
    std::map<QualifiedTableName, TableInfo> table_infos TSA_GUARDED_BY(mutex);
    TablesDependencyGraph tables_dependencies TSA_GUARDED_BY(mutex);

    std::vector<std::future<void>> futures TSA_GUARDED_BY(mutex);
    std::atomic<bool> exception_caught = false;
    ThreadPool & thread_pool;
    mutable std::mutex mutex;
};

}
