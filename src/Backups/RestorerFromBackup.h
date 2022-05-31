#pragma once

#include <Backups/RestoreSettings.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/TableLockHolder.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreCoordination;
struct StorageID;

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
        std::chrono::seconds timeout_ = std::chrono::seconds(-1) /* no timeout */);

    ~RestorerFromBackup();

    /// Restores the definition of databases and tables and prepares tasks to restore the data of the tables.
    void restoreMetadata();

    using DataRestoreTask = std::function<void()>;
    using DataRestoreTasks = std::vector<DataRestoreTask>;
    DataRestoreTasks getDataRestoreTasks();

    BackupPtr getBackup() const { return backup; }
    const RestoreSettings & getRestoreSettings() const { return restore_settings; }
    bool isNonEmptyTableAllowed() const { return getRestoreSettings().allow_non_empty_tables; }
    std::shared_ptr<IRestoreCoordination> getRestoreCoordination() const { return restore_coordination; }
    std::chrono::seconds getTimeout() const { return timeout; }
    ContextMutablePtr getContext() const { return context; }
    void executeCreateQuery(const ASTPtr & create_query) const;

    /// Adds a data restore task which will be later returned by getDataRestoreTasks().
    /// This function can be called by implementations of IStorage::restoreFromBackup() in inherited storage classes.
    void addDataRestoreTask(StoragePtr storage, DataRestoreTask && data_restore_task);
    void addDataRestoreTasks(StoragePtr storage, DataRestoreTasks && data_restore_task);

    /// Reading a backup includes a few stages:
    enum class Stage
    {
        /// Initial stage.
        kPreparing,

        /// Finding databases and tables in the backup which we're going to restore.
        kFindingTablesInBackup,

        /// Creating databases or finding them and checking their definitions.
        kCreatingDatabases,

        /// Creating tables or finding them and checking their definition.
        kCreatingTables,

        /// Inserting restored data to tables.
        kInsertingDataToTables,

        /// An error happens during any of the stages above, the backup is not restored properly.
        kError = -1,
    };
    static std::string_view toString(Stage stage);
    
    /// Throws an exception that a specified table engine doesn't support partitions.
    [[noreturn]] static void throwPartitionsNotSupported(const StorageID & storage_id, const String & table_engine);

    /// Throws an exception that a specified table is already non-empty.
    [[noreturn]] static void throwTableIsNotEmpty(const StorageID & storage_id);

private:
    const ASTBackupQuery::Elements restore_query_elements;
    const RestoreSettings restore_settings;
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    BackupPtr backup;
    ContextMutablePtr context;
    std::chrono::seconds timeout;
    Poco::Logger * log;

    Stage current_stage = Stage::kPreparing;
    Strings root_paths_in_backup;
    DDLRenamingSettings renaming_settings;

    void setStage(Stage new_stage, const String & error_message = {});
    void findRootPathsInBackup();
    void collectDatabaseAndTableInfos();
    void collectTableInfo(const DatabaseAndTableName & table_name_in_backup, const std::optional<ASTs> & partitions);
    void collectDatabaseInfo(const String & database_name_in_backup, const std::set<String> & except_table_names);
    void collectAllDatabasesInfo(const std::set<String> & except_database_names);
    void createDatabases();
    void createTables();

    struct DatabaseInfo
    {
        ASTPtr create_database_query;
    };

    std::unordered_map<String, DatabaseInfo> database_infos;

    struct TableInfo
    {
        ASTPtr create_table_query;
        std::optional<ASTs> partitions;
        String data_path_in_backup;
    };

    std::map<DatabaseAndTableName, TableInfo> table_infos;

    std::map<StoragePtr, TableLockHolder> table_locks;
    std::map<StoragePtr, std::vector<DataRestoreTask>> data_restore_tasks;
};

}
