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
enum class AccessEntityType;

/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : private boost::noncopyable
{
public:
    BackupEntriesCollector(const ASTBackupQuery::Elements & backup_query_elements_,
                           const BackupSettings & backup_settings_,
                           std::shared_ptr<IBackupCoordination> backup_coordination_,
                           const ContextPtr & context_);
    ~BackupEntriesCollector();

    /// Collects backup entries and returns the result.
    /// This function first generates a list of databases and then call IDatabase::getTablesForBackup() for each database from this list.
    /// Then it calls IStorage::backupData() to build a list of backup entries.
    BackupEntries run();

    const BackupSettings & getBackupSettings() const { return backup_settings; }
    std::shared_ptr<IBackupCoordination> getBackupCoordination() const { return backup_coordination; }
    ContextPtr getContext() const { return context; }

    /// Adds a backup entry which will be later returned by run().
    /// These function can be called by implementations of IStorage::backupData() in inherited storage classes.
    void addBackupEntry(const String & file_name, BackupEntryPtr backup_entry);
    void addBackupEntry(const std::pair<String, BackupEntryPtr> & backup_entry);
    void addBackupEntries(const BackupEntries & backup_entries_);
    void addBackupEntries(BackupEntries && backup_entries_);

    /// Adds a function which must be called after all IStorage::backupData() have finished their work on all hosts.
    /// This function is designed to help making a consistent in some complex cases like
    /// 1) we need to join (in a backup) the data of replicated tables gathered on different hosts.
    void addPostTask(std::function<void()> task);

    /// Returns an incremental counter used to backup access control.
    size_t getAccessCounter(AccessEntityType type);

private:
    void calculateRootPathInBackup();

    void gatherMetadataAndCheckConsistency();

    bool tryGatherMetadataAndCompareWithPrevious(std::optional<Exception> & inconsistency_error);

    void gatherDatabasesMetadata();

    void gatherDatabaseMetadata(
        const String & database_name,
        bool throw_if_database_not_found,
        bool backup_create_database_query,
        const std::optional<String> & table_name,
        bool throw_if_table_not_found,
        const std::optional<ASTs> & partitions,
        bool all_tables,
        const std::set<DatabaseAndTableName> & except_table_names);

    void gatherTablesMetadata();
    std::vector<std::pair<ASTPtr, StoragePtr>> findTablesInDatabase(const String & database_name) const;
    void lockTablesForReading();
    bool compareWithPrevious(std::optional<Exception> & inconsistency_error);

    void makeBackupEntriesForDatabasesDefs();
    void makeBackupEntriesForTablesDefs();
    void makeBackupEntriesForTablesData();
    void makeBackupEntriesForTableData(const QualifiedTableName & table_name);

    void runPostTasks();

    Strings setStatus(const String & new_status, const String & message = "");

    const ASTBackupQuery::Elements backup_query_elements;
    const BackupSettings backup_settings;
    std::shared_ptr<IBackupCoordination> backup_coordination;
    ContextPtr context;
    std::chrono::milliseconds consistent_metadata_snapshot_timeout;
    Poco::Logger * log;

    Strings all_hosts;
    DDLRenamingMap renaming_map;
    std::filesystem::path root_path_in_backup;

    struct DatabaseInfo
    {
        DatabasePtr database;
        ASTPtr create_database_query;
        String metadata_path_in_backup;

        struct TableParams
        {
            bool throw_if_table_not_found = false;
            std::optional<ASTs> partitions;
        };

        std::unordered_map<String, TableParams> tables;

        bool all_tables = false;
        std::unordered_set<String> except_table_names;
    };

    struct TableInfo
    {
        DatabasePtr database;
        StoragePtr storage;
        TableLockHolder table_lock;
        ASTPtr create_table_query;
        String metadata_path_in_backup;
        std::filesystem::path data_path_in_backup;
        std::optional<String> replicated_table_shared_id;
        std::optional<ASTs> partitions;
    };

    String current_status;
    std::chrono::steady_clock::time_point consistent_metadata_snapshot_start_time;
    std::unordered_map<String, DatabaseInfo> database_infos;
    std::unordered_map<QualifiedTableName, TableInfo> table_infos;
    std::vector<std::pair<String, String>> previous_databases_metadata;
    std::vector<std::pair<QualifiedTableName, String>> previous_tables_metadata;

    BackupEntries backup_entries;
    std::queue<std::function<void()>> post_tasks;
    std::vector<size_t> access_counters;
};

}
