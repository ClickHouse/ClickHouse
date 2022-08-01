#pragma once

#include <Backups/RestoreSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/TableLockHolder.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
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
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;


/// Restores the definition of databases and tables and prepares tasks to restore the data of the tables.
class RestorerFromBackup : private boost::noncopyable
{
public:
    RestorerFromBackup(
        const ASTBackupQuery::Elements & restore_query_elements_,
        const RestoreSettings & restore_settings_,
        std::shared_ptr<IRestoreCoordination> restore_coordination_,
        const BackupPtr & backup_,
        const ContextMutablePtr & context_);

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
    DataRestoreTasks run(Mode mode);

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
    std::vector<std::pair<UUID, AccessEntityPtr>> getAccessEntitiesToRestore();

    /// Throws an exception that a specified table is already non-empty.
    [[noreturn]] static void throwTableIsNotEmpty(const StorageID & storage_id);

private:
    const ASTBackupQuery::Elements restore_query_elements;
    const RestoreSettings restore_settings;
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    BackupPtr backup;
    ContextMutablePtr context;
    std::chrono::milliseconds on_cluster_first_sync_timeout;
    std::chrono::milliseconds create_table_timeout;
    Poco::Logger * log;

    Strings all_hosts;
    DDLRenamingMap renaming_map;
    std::vector<std::filesystem::path> root_paths_in_backup;

    void findRootPathsInBackup();

    void findDatabasesAndTablesInBackup();
    void findTableInBackup(const QualifiedTableName & table_name_in_backup, const std::optional<ASTs> & partitions);
    void findDatabaseInBackup(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names);
    void findEverythingInBackup(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names);

    void checkAccessForObjectsFoundInBackup() const;

    void createDatabases();
    void createDatabase(const String & database_name) const;
    void checkDatabase(const String & database_name);

    void createTables();
    void createTable(const QualifiedTableName & table_name);
    void checkTable(const QualifiedTableName & table_name);
    void insertDataToTable(const QualifiedTableName & table_name);

    DataRestoreTasks getDataRestoreTasks();

    void setStage(const String & new_stage, const String & message = "");

    struct DatabaseInfo
    {
        ASTPtr create_database_query;
        bool is_predefined_database = false;
        DatabasePtr database;
    };

    struct TableInfo
    {
        ASTPtr create_table_query;
        bool is_predefined_table = false;
        std::unordered_set<QualifiedTableName> dependencies;
        bool has_data = false;
        std::filesystem::path data_path_in_backup;
        std::optional<ASTs> partitions;
        DatabasePtr database;
        StoragePtr storage;
        TableLockHolder table_lock;
    };

    std::vector<QualifiedTableName> findTablesWithoutDependencies() const;

    String current_stage;
    std::unordered_map<String, DatabaseInfo> database_infos;
    std::map<QualifiedTableName, TableInfo> table_infos;
    std::vector<DataRestoreTask> data_restore_tasks;
    std::unique_ptr<AccessRestorerFromBackup> access_restorer;
    bool access_restored = false;
};

}
