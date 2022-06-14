#pragma once

#include <Backups/BackupSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <filesystem>


namespace DB
{

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
class IBackupCoordination;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
struct StorageID;

/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : private boost::noncopyable
{
public:
    BackupEntriesCollector(const ASTBackupQuery::Elements & backup_query_elements_,
                           const BackupSettings & backup_settings_,
                           std::shared_ptr<IBackupCoordination> backup_coordination_,
                           const ContextPtr & context_,
                           std::chrono::seconds timeout_ = std::chrono::seconds(-1) /* no timeout */);
    ~BackupEntriesCollector();

    /// Collects backup entries and returns the result.
    /// This function first generates a list of databases and then call IDatabase::backup() for each database from this list.
    /// At this moment IDatabase::backup() calls IStorage::backup() and they both call addBackupEntry() to build a list of backup entries.
    BackupEntries getBackupEntries();

    const BackupSettings & getBackupSettings() const { return backup_settings; }
    std::shared_ptr<IBackupCoordination> getBackupCoordination() const { return backup_coordination; }
    ContextPtr getContext() const { return context; }

    /// Adds a backup entry which will be later returned by getBackupEntries().
    /// These function can be called by implementations of IStorage::backup() in inherited storage classes.
    void addBackupEntry(const String & file_name, BackupEntryPtr backup_entry);
    void addBackupEntries(const BackupEntries & backup_entries_);
    void addBackupEntries(BackupEntries && backup_entries_);

    /// Adds a function which must be called after all IStorage::backup() have finished their work on all hosts.
    /// This function is designed to help making a consistent in some complex cases like
    /// 1) we need to join (in a backup) the data of replicated tables gathered on different hosts.
    void addPostCollectingTask(std::function<void()> task);

    /// Writing a backup includes a few stages:
    enum class Stage
    {
        /// Initial stage.
        kPreparing,

        /// Finding all tables and databases which we're going to put to the backup.
        kFindingTables,

        /// Making temporary hard links and prepare backup entries.
        kExtractingDataFromTables,

        /// Running special tasks for replicated databases or tables which can also prepare some backup entries.
        kRunningPostTasks,

        /// Writing backup entries to the backup and removing temporary hard links.
        kWritingBackup,

        /// An error happens during any of the stages above, the backup won't be written.
        kError,
    };
    static std::string_view toString(Stage stage);

    /// Throws an exception that a specified table engine doesn't support partitions.
    [[noreturn]] static void throwPartitionsNotSupported(const StorageID & storage_id, const String & table_engine);

private:
    void setStage(Stage new_stage, const String & error_message = {});
    void calculateRootPathInBackup();
    void collectDatabasesAndTablesInfo();
    void collectTableInfo(const QualifiedTableName & table_name, bool is_temporary_table, const std::optional<ASTs> & partitions, bool throw_if_not_found);
    void collectDatabaseInfo(const String & database_name, const std::set<DatabaseAndTableName> & except_table_names, bool throw_if_not_found);
    void collectAllDatabasesInfo(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names);
    void checkConsistency();
    void makeBackupEntriesForDatabasesDefs();
    void makeBackupEntriesForTablesDefs();
    void makeBackupEntriesForTablesData();
    void runPostCollectingTasks();

    const ASTBackupQuery::Elements backup_query_elements;
    const BackupSettings backup_settings;
    std::shared_ptr<IBackupCoordination> backup_coordination;
    ContextPtr context;
    std::chrono::seconds timeout;
    Poco::Logger * log;

    Stage current_stage = Stage::kPreparing;
    std::filesystem::path root_path_in_backup;
    DDLRenamingMap renaming_map;

    struct DatabaseInfo
    {
        DatabasePtr database;
        ASTPtr create_database_query;
    };

    struct TableInfo
    {
        DatabasePtr database;
        StoragePtr storage;
        TableLockHolder table_lock;
        ASTPtr create_table_query;
        std::filesystem::path data_path_in_backup;
        std::optional<ASTs> partitions;
    };

    struct TableKey
    {
        QualifiedTableName name;
        bool is_temporary = false;
        bool operator ==(const TableKey & right) const;
        bool operator <(const TableKey & right) const;
    };

    std::unordered_map<String, DatabaseInfo> database_infos;
    std::map<TableKey, TableInfo> table_infos;
    std::optional<std::set<String>> previous_database_names;
    std::optional<std::set<TableKey>> previous_table_names;
    bool consistent = false;
    
    BackupEntries backup_entries;
    std::queue<std::function<void()>> post_collecting_tasks;
};

}
