#pragma once

#include <Backups/BackupSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <filesystem>
#include <queue>


namespace DB
{

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
class IBackupCoordination;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
struct StorageID;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;


/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : private boost::noncopyable
{
public:
    BackupEntriesCollector(const ASTBackupQuery::Elements & backup_query_elements_,
                           const BackupSettings & backup_settings_,
                           std::shared_ptr<IBackupCoordination> backup_coordination_,
                           const ReadSettings & read_settings_,
                           const ContextPtr & context_,
                           ThreadPool & threadpool_);
    ~BackupEntriesCollector();

    /// Collects backup entries and returns the result.
    /// This function first generates a list of databases and then call IDatabase::getTablesForBackup() for each database from this list.
    /// Then it calls IStorage::backupData() to build a list of backup entries.
    BackupEntries run();

    const BackupSettings & getBackupSettings() const { return backup_settings; }
    std::shared_ptr<IBackupCoordination> getBackupCoordination() const { return backup_coordination; }
    const ReadSettings & getReadSettings() const { return read_settings; }
    ContextPtr getContext() const { return context; }
    const ZooKeeperRetriesInfo & getZooKeeperRetriesInfo() const { return global_zookeeper_retries_info; }

    /// Returns all access entities which can be put into a backup.
    std::unordered_map<UUID, AccessEntityPtr> getAllAccessEntities();

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

private:
    void calculateRootPathInBackup();

    void gatherMetadataAndCheckConsistency();

    void tryGatherMetadataAndCompareWithPrevious(int attempt_no, std::optional<Exception> & inconsistency_error, bool & need_another_attempt);
    void syncMetadataGatheringWithOtherHosts(int attempt_no, std::optional<Exception> & inconsistency_error, bool & need_another_attempt);

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
    bool compareWithPrevious(String & mismatch_description);

    void makeBackupEntriesForDatabasesDefs();
    void makeBackupEntriesForTablesDefs();
    void makeBackupEntriesForTablesData();
    void makeBackupEntriesForTableData(const QualifiedTableName & table_name);

    void addBackupEntryUnlocked(const String & file_name, BackupEntryPtr backup_entry);

    void runPostTasks();

    Strings setStage(const String & new_stage, const String & message = "");

    /// Throws an exception if the BACKUP query was cancelled.
    void checkIsQueryCancelled() const;

    const ASTBackupQuery::Elements backup_query_elements;
    const BackupSettings backup_settings;
    std::shared_ptr<IBackupCoordination> backup_coordination;
    const ReadSettings read_settings;
    ContextPtr context;
    QueryStatusPtr process_list_element;

    /// The time a BACKUP ON CLUSTER or RESTORE ON CLUSTER command will wait until all the nodes receive the BACKUP (or RESTORE) query and start working.
    /// This setting is similar to `distributed_ddl_task_timeout`.
    const std::chrono::milliseconds on_cluster_first_sync_timeout;

    /// The time a BACKUP command will try to collect the metadata of tables & databases.
    const std::chrono::milliseconds collect_metadata_timeout;

    /// The number of attempts to collect the metadata before sleeping.
    const unsigned int attempts_to_collect_metadata_before_sleep;

    /// The minimum time clickhouse will wait after unsuccessful attempt before trying to collect the metadata again.
    const std::chrono::milliseconds min_sleep_before_next_attempt_to_collect_metadata;

    /// The maximum time clickhouse will wait after unsuccessful attempt before trying to collect the metadata again.
    const std::chrono::milliseconds max_sleep_before_next_attempt_to_collect_metadata;

    /// Whether we should collect the metadata after a successful attempt one more time and check that nothing has changed.
    const bool compare_collected_metadata;

    LoggerPtr log;
    /// Unfortunately we can use ZooKeeper for collecting information for backup
    /// and we need to retry...
    ZooKeeperRetriesInfo global_zookeeper_retries_info;

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
        std::optional<String> replicated_table_zk_path;
        std::optional<ASTs> partitions;
    };

    String current_stage;

    std::chrono::steady_clock::time_point collect_metadata_end_time;

    std::unordered_map<String, DatabaseInfo> database_infos;
    std::unordered_map<QualifiedTableName, TableInfo> table_infos;
    std::vector<std::pair<String, String>> previous_databases_metadata;
    std::vector<std::pair<QualifiedTableName, String>> previous_tables_metadata;

    std::optional<std::unordered_map<UUID, AccessEntityPtr>> all_access_entities;

    BackupEntries backup_entries;
    std::queue<std::function<void()>> post_tasks;

    ThreadPool & threadpool;
    std::mutex mutex;
};

}
