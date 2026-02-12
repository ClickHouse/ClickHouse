#pragma once

#include <Backups/BackupMetadataFinder.h>
#include <Backups/RestoreSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>

#include <filesystem>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreCoordination;
struct StorageID;
class AccessRestorerFromBackup;
struct AccessEntitiesToRestore;


/// Restores the definition of databases and tables and prepares tasks to restore the data of the tables.
class RestorerFromBackup : public BackupMetadataFinder
{
public:
    RestorerFromBackup(
        const ASTBackupQuery::Elements & restore_query_elements_,
        const RestoreSettings & restore_settings_,
        std::shared_ptr<IRestoreCoordination> restore_coordination_,
        const BackupPtr & backup_,
        const ContextMutablePtr & context_,
        const ContextPtr & query_context_,
        ThreadPool & thread_pool_,
        const std::function<void()> & after_task_callback_);

    ~RestorerFromBackup() override;

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
    void run(Mode mode_);

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
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    std::function<void()> after_task_callback;
    std::chrono::milliseconds create_table_timeout;

    const ZooKeeperRetriesInfo zookeeper_retries_info;
    Mode mode = Mode::RESTORE;
    Strings all_hosts;

    void findDatabasesAndTablesInBackup();

    void logNumberOfDatabasesAndTablesToRestore() const;

    void loadSystemAccessTables();
    void checkAccessForObjectsFoundInBackup() const;

    void createAndCheckDatabases();
    void createAndCheckDatabase(const String & database_name);
    void createAndCheckDatabaseImpl(const String & database_name);
    void createDatabase(const String & database_name) const;
    void checkDatabase(const String & database_name);

    void applyCustomStoragePolicy(ASTPtr query_ptr) override;

    void removeUnresolvedDependencies();

    void createAndCheckTables();
    void createAndCheckTablesWithSameDependencyLevel(const std::vector<StorageID> & table_ids);
    void createAndCheckTable(const QualifiedTableName & table_name);
    void createAndCheckTableImpl(const QualifiedTableName & table_name);
    void createTable(const QualifiedTableName & table_name);
    void checkTable(const QualifiedTableName & table_name);

    void insertDataToTables();
    void insertDataToTable(const QualifiedTableName & table_name);
    void insertDataToTableImpl(const QualifiedTableName & table_name, StoragePtr storage, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    void runDataRestoreTasks();

    void finalizeTables();

    void setStage(const String & new_stage, const String & message = "");

    /// Override to add after_task_callback support
    void schedule(std::function<void()> && task_, ThreadName thread_name_) override;

    String current_stage;

    std::vector<DataRestoreTask> data_restore_tasks TSA_GUARDED_BY(mutex);
    std::unique_ptr<AccessRestorerFromBackup> access_restorer TSA_GUARDED_BY(mutex);
};

}
