#pragma once

#include <Backups/RestoreSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/TablesDependencyGraph.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/TableLockHolder.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ThreadPool_fwd.h>
#include <filesystem>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreCoordination;
struct StorageID;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
class AccessRestorerFromBackup;
struct AccessEntitiesToRestore;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;


/// Restores the definition of databases and tables and prepares tasks to restore the data of the tables.
class RestorerFromBackup : private boost::noncopyable
{
public:
    RestorerFromBackup(
        const ASTBackupQuery::Elements & restore_query_elements_,
        const RestoreSettings & restore_settings_,
        std::shared_ptr<IRestoreCoordination> restore_coordination_,
        const BackupPtr & backup_,
        const ContextMutablePtr & context_,
        ThreadPool & thread_pool_,
        const std::function<void()> & after_task_callback_);

    ~RestorerFromBackup();

    enum Mode
    {
        /// Restores databases and tables.
        RESTORE,

        /// Only checks access rights without restoring anything.
        CHECK_ACCESS_ONLY
    };

    using DataRestoreTask = std::function<void()>;
    using DataRestoreTasks = std::vector<DataRestoreTask>;

    /// Restores the metadata of databases and tables and returns tasks to restore the data of tables.
    void run(Mode mode);

    BackupPtr getBackup() const { return backup; }
    const RestoreSettings & getRestoreSettings() const { return restore_settings; }
    bool isNonEmptyTableAllowed() const { return getRestoreSettings().allow_non_empty_tables; }
    std::shared_ptr<IRestoreCoordination> getRestoreCoordination() const { return restore_coordination; }
    ContextMutablePtr getContext() const { return context; }

    /// Adds a data restore task which will be later returned by getDataRestoreTasks().
    /// This function can be called by implementations of IStorage::restoreFromBackup() in inherited storage classes.
    void addDataRestoreTask(DataRestoreTask && new_task);
    void addDataRestoreTasks(DataRestoreTasks && new_tasks);

    /// Returns the list of access entities to restore.
    AccessEntitiesToRestore getAccessEntitiesToRestore(const String & data_path_in_backup) const;

    /// Throws an exception that a specified table is already non-empty.
    [[noreturn]] static void throwTableIsNotEmpty(const StorageID & storage_id);

private:
    const ASTBackupQuery::Elements restore_query_elements;
    const RestoreSettings restore_settings;
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    BackupPtr backup;
    ContextMutablePtr context;
    QueryStatusPtr process_list_element;
    std::function<void()> after_task_callback;
    std::chrono::milliseconds on_cluster_first_sync_timeout;
    std::chrono::milliseconds create_table_timeout;
    LoggerPtr log;

    Strings all_hosts;
    DDLRenamingMap renaming_map;
    std::vector<std::filesystem::path> root_paths_in_backup;

    void findRootPathsInBackup();

    void findDatabasesAndTablesInBackup();
    void findTableInBackup(const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions);
    void findTableInBackupImpl(const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions);
    void findDatabaseInBackup(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names);
    void findDatabaseInBackupImpl(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names);
    void findEverythingInBackup(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names);

    size_t getNumDatabases() const;
    size_t getNumTables() const;

    void loadSystemAccessTables();
    void checkAccessForObjectsFoundInBackup() const;

    void createDatabases();
    void createDatabase(const String & database_name) const;
    void checkDatabase(const String & database_name);

    void applyCustomStoragePolicy(ASTPtr query_ptr);

    void removeUnresolvedDependencies();
    void createTables();
    void createTable(const QualifiedTableName & table_name);
    void checkTable(const QualifiedTableName & table_name);

    void insertDataToTables();
    void insertDataToTable(const QualifiedTableName & table_name);
    void insertDataToTableImpl(const QualifiedTableName & table_name, StoragePtr storage, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    void runDataRestoreTasks();

    void setStage(const String & new_stage, const String & message = "");

    /// Schedule a task from the thread pool and start executing it.
    void schedule(std::function<void()> && task_, const char * thread_name_);

    /// Returns the number of currently scheduled or executing tasks.
    size_t getNumFutures() const;

    /// Waits until all tasks are processed (including the tasks scheduled while we're waiting).
    /// Throws an exception if any of the tasks throws an exception.
    void waitFutures(bool throw_if_error = true);

    /// Throws an exception if the RESTORE query was cancelled.
    void checkIsQueryCancelled() const;

    struct DatabaseInfo
    {
        ASTPtr create_database_query;
        String create_database_query_str;
        bool is_predefined_database = false;
        DatabasePtr database;
    };

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
    };

    String current_stage;

    std::unordered_map<String, DatabaseInfo> database_infos TSA_GUARDED_BY(mutex);
    std::map<QualifiedTableName, TableInfo> table_infos TSA_GUARDED_BY(mutex);
    TablesDependencyGraph tables_dependencies TSA_GUARDED_BY(mutex);
    std::vector<DataRestoreTask> data_restore_tasks TSA_GUARDED_BY(mutex);
    std::unique_ptr<AccessRestorerFromBackup> access_restorer TSA_GUARDED_BY(mutex);
    bool access_restored TSA_GUARDED_BY(mutex) = false;

    std::vector<std::future<void>> futures TSA_GUARDED_BY(mutex);
    std::atomic<bool> exception_caught = false;
    ThreadPool & thread_pool;
    mutable std::mutex mutex;
};

}
