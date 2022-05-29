#pragma once

#include <Backups/BackupSettings.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Parsers/ASTBackupQuery.h>


namespace DB
{

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
class IBackupCoordination;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : public std::enable_shared_from_this<BackupEntriesCollector>
{
public:
    static std::shared_ptr<BackupEntriesCollector> create(
        const ASTBackupQuery::Elements & backup_query_elements_,
        const BackupSettings & backup_settings_,
        std::shared_ptr<IBackupCoordination> backup_coordination_,
        const ContextPtr & context_,
        std::chrono::seconds timeout_ = std::chrono::seconds(-1) /* no timeout */);

    /// Collects backup entries and returns the result.
    /// This function first generates a list of databases and then call IDatabase::backup() for each database from this list.
    /// At this moment IDatabase::backup() calls IStorage::backup() and they both call addBackupEntry() to build a list of backup entries.
    BackupEntries collectBackupEntries();

    const BackupSettings & getBackupSettings() const { return backup_settings; }
    std::shared_ptr<IBackupCoordination> getBackupCoordination() const { return backup_coordination; }
    ContextPtr getContext() const { return context; }

    /// Adds a backup entry, this function must be called from implementations of IDatabase::backup() and IStorage::backup().
    void addBackupEntry(const String & file_name, BackupEntryPtr backup_entry);
    void addBackupEntries(const BackupEntries & backup_entries_);
    void addBackupEntries(BackupEntries && backup_entries_);

    /// Adds a backup entry to backup a specified table or database's metadata.
    void addBackupEntryForCreateQuery(const ASTPtr & create_query);

    /// Generates a path in the backup to store table's data.
    String getDataPathInBackup(const ASTPtr & create_query) const;

    /// Adds a function which must be called after all IDatabase::backup() and IStorage::backup() have finished their work on all hosts.
    /// This function is used in complex cases:
    /// 1) we need to join (in a backup) the data of replicated tables gathered on different hosts.
    void addPostCollectingTask(std::function<void()> task);

    ~BackupEntriesCollector();

private:
    BackupEntriesCollector(const ASTBackupQuery::Elements & backup_query_elements_,
                           const BackupSettings & backup_settings_,
                           std::shared_ptr<IBackupCoordination> backup_coordination_,
                           const ContextPtr & context_,
                           std::chrono::seconds timeout_);

    void calculateRootPathInBackup();
    void prepareDatabaseInfos();
    void backupDatabases();
    void runPostCollectingTasks();

    const ASTBackupQuery::Elements backup_query_elements;
    const BackupSettings backup_settings;
    const std::shared_ptr<IBackupCoordination> backup_coordination;
    const ContextPtr context;
    const std::chrono::seconds timeout;
    String root_path_in_backup;
    DDLRenamingSettings renaming_settings;

    struct DatabaseInfo
    {
        DatabasePtr database;
        ASTPtr create_database_query;
        std::unordered_set<String> table_names;
        bool all_tables = false;
        std::unordered_set<String> except_table_names;
        std::unordered_map<String, ASTs> partitions;
    };

    std::unordered_map<String, DatabaseInfo> database_infos;
    BackupEntries backup_entries;
    std::queue<std::function<void()>> post_collecting_tasks;
    mutable std::mutex mutex;

    std::atomic<bool> collecting_backup_entries = false;
    std::atomic<bool> allow_adding_entries_or_tasks = false;
};

}
