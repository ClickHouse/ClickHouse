#include <Access/Common/AccessEntityType.h>
#include <Access/AccessControl.h>
#include <Backups/BackupCoordinationStage.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Backups/IBackupCoordination.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/extractZooKeeperPathFromReplicatedTableDef.h>
#include <base/chrono_io.h>
#include <base/insertAtEnd.h>
#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event BackupEntriesCollectorMicroseconds;
    extern const Event BackupEntriesCollectorForTablesDataMicroseconds;
    extern const Event BackupEntriesCollectorRunPostTasksMicroseconds;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 backup_restore_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_max_retries;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int INCONSISTENT_METADATA_FOR_BACKUP;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}


namespace Stage = BackupCoordinationStage;

namespace
{
    /// Outputs "table <name>" or "temporary table <name>"
    String tableNameWithTypeToString(const String & database_name, const String & table_name, bool first_upper)
    {
        String str;
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            str = fmt::format("temporary table {}", backQuoteIfNeed(table_name));
        else
            str = fmt::format("table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
        if (first_upper)
            str[0] = std::toupper(str[0]);
        return str;
    }

    String tableNameWithTypeToString(const QualifiedTableName & table_name, bool first_upper)
    {
        return tableNameWithTypeToString(table_name.database, table_name.table, first_upper);
    }

    /// How long we should sleep after finding an inconsistency error.
    std::chrono::milliseconds getSleepTimeAfterInconsistencyError(int attempt_no, unsigned int attempts_before_sleep, std::chrono::milliseconds min_sleep, std::chrono::milliseconds max_sleep)
    {
        attempts_before_sleep = std::max(attempts_before_sleep, 1U);
        if (((attempt_no + 1) % attempts_before_sleep) != 0)
            return std::chrono::milliseconds::zero(); /// no sleep

        int sleep_counter = attempt_no / attempts_before_sleep;
        std::chrono::milliseconds sleep_time = intExp2(std::min(sleep_counter, 10)) * min_sleep;
        return std::min(sleep_time, max_sleep);
    }
}


BackupEntriesCollector::BackupEntriesCollector(
    const ASTBackupQuery::Elements & backup_query_elements_,
    const BackupSettings & backup_settings_,
    std::shared_ptr<IBackupCoordination> backup_coordination_,
    const ReadSettings & read_settings_,
    const ContextPtr & context_,
    ThreadPool & threadpool_)
    : backup_query_elements(backup_query_elements_)
    , backup_settings(backup_settings_)
    , backup_coordination(backup_coordination_)
    , read_settings(read_settings_)
    , context(context_)
    , process_list_element(context->getProcessListElement())
    , collect_metadata_timeout(context->getConfigRef().getUInt64(
          "backups.collect_metadata_timeout", context->getConfigRef().getUInt64("backups.consistent_metadata_snapshot_timeout", 600000)))
    , attempts_to_collect_metadata_before_sleep(context->getConfigRef().getUInt("backups.attempts_to_collect_metadata_before_sleep", 2))
    , min_sleep_before_next_attempt_to_collect_metadata(
          context->getConfigRef().getUInt64("backups.min_sleep_before_next_attempt_to_collect_metadata", 100))
    , max_sleep_before_next_attempt_to_collect_metadata(
          context->getConfigRef().getUInt64("backups.max_sleep_before_next_attempt_to_collect_metadata", 5000))
    , compare_collected_metadata(context->getConfigRef().getBool("backups.compare_collected_metadata", true))
    , log(getLogger("BackupEntriesCollector"))
    , global_zookeeper_retries_info(
          context->getSettingsRef()[Setting::backup_restore_keeper_max_retries],
          context->getSettingsRef()[Setting::backup_restore_keeper_retry_initial_backoff_ms],
          context->getSettingsRef()[Setting::backup_restore_keeper_retry_max_backoff_ms])
    , threadpool(threadpool_)
{
}

BackupEntriesCollector::~BackupEntriesCollector() = default;

BackupEntries BackupEntriesCollector::run()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupEntriesCollectorMicroseconds);

    /// run() can be called onle once.
    if (!current_stage.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Already making backup entries");

    /// Find other hosts working along with us to execute this ON CLUSTER query.
    all_hosts
        = BackupSettings::Util::filterHostIDs(backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num);

    /// Do renaming in the create queries according to the renaming config.
    renaming_map = BackupUtils::makeRenamingMap(backup_query_elements);

    /// Calculate the root path for collecting backup entries, it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
    calculateRootPathInBackup();

    /// Find databases and tables which we're going to put to the backup.
    gatherMetadataAndCheckConsistency();

    /// Make backup entries for the definitions of the found databases.
    makeBackupEntriesForDatabasesDefs();

    /// Make backup entries for the definitions of the found tables.
    makeBackupEntriesForTablesDefs();

    /// Make backup entries for the data of the found tables.
    setStage(Stage::EXTRACTING_DATA_FROM_TABLES);

    {
        auto timer2 = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupEntriesCollectorForTablesDataMicroseconds);
        makeBackupEntriesForTablesData();
    }

    /// Run all the tasks added with addPostCollectingTask().
    setStage(Stage::RUNNING_POST_TASKS);

    {
        auto timer2 = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupEntriesCollectorRunPostTasksMicroseconds);
        runPostTasks();
    }

    /// No more backup entries or tasks are allowed after this point.

    return std::move(backup_entries);
}

Strings BackupEntriesCollector::setStage(const String & new_stage, const String & message)
{
    LOG_TRACE(log, "Setting stage: {}", new_stage);
    checkIsQueryCancelled();

    current_stage = new_stage;
    return backup_coordination->setStage(new_stage, message, /* sync = */ true);
}

void BackupEntriesCollector::checkIsQueryCancelled() const
{
    if (process_list_element)
        process_list_element->checkTimeLimit();
}

/// Calculates the root path for collecting backup entries,
/// it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
void BackupEntriesCollector::calculateRootPathInBackup()
{
    root_path_in_backup = "/";
    if (!backup_settings.host_id.empty())
    {
        auto [shard_num, replica_num]
            = BackupSettings::Util::findShardNumAndReplicaNum(backup_settings.cluster_host_ids, backup_settings.host_id);
        root_path_in_backup = root_path_in_backup / fs::path{"shards"} / std::to_string(shard_num) / "replicas" / std::to_string(replica_num);
    }
    LOG_TRACE(log, "Will use path in backup: {}", doubleQuoteString(String{root_path_in_backup}));
}

/// Finds databases and tables which we will put to the backup.
void BackupEntriesCollector::gatherMetadataAndCheckConsistency()
{
    /// With the default values the metadata collecting works in the following way:
    /// 1) it tries to collect the metadata for the first time; then
    /// 2) it tries to collect it again, and compares the results from the first and the second collecting; if they match, it's done; otherwise
    /// 3) it sleeps 100 millisecond and tries to collect again, then compares the results from the second and the third collecting; if they match, it's done; otherwise
    /// 4) it tries to collect again, then compares the results from the third and the fourth collecting; if they match, it's done; otherwise
    /// 5) it sleeps 200 milliseconds and tries to collect again, then compares the results from the fourth and the fifth collecting; if they match, it's done;
    /// ...
    /// and so on, the sleep time is doubled each time until it reaches 5000 milliseconds.
    /// And such attempts will be continued until 600000 milliseconds pass.

    setStage(Stage::formatGatheringMetadata(0));

    collect_metadata_end_time = std::chrono::steady_clock::now() + collect_metadata_timeout;

    for (int attempt_no = 0;; ++attempt_no)
    {
        std::optional<Exception> inconsistency_error;
        bool need_another_attempt = false;
        tryGatherMetadataAndCompareWithPrevious(attempt_no, inconsistency_error, need_another_attempt);
        syncMetadataGatheringWithOtherHosts(attempt_no, inconsistency_error, need_another_attempt);

        if (inconsistency_error && (std::chrono::steady_clock::now() > collect_metadata_end_time))
            inconsistency_error->rethrow();

        /// Rethrow or just log the inconsistency error.
        if (!need_another_attempt)
            break;

        /// It's going to be another attempt, we need to sleep a bit.
        if (inconsistency_error)
        {
            auto sleep_time = getSleepTimeAfterInconsistencyError(
                attempt_no,
                attempts_to_collect_metadata_before_sleep,
                min_sleep_before_next_attempt_to_collect_metadata,
                max_sleep_before_next_attempt_to_collect_metadata);
            if (sleep_time.count())
            {
                LOG_TRACE(log, "Sleeping {} before next attempt to collect metadata", to_string(sleep_time));
                sleepForMilliseconds(std::chrono::duration_cast<std::chrono::milliseconds>(sleep_time).count());
            }
        }
    }

    /// All hosts managed to gather metadata and everything is consistent, so we can go further to writing the backup.
    LOG_INFO(log, "Will backup {} databases and {} tables", database_infos.size(), table_infos.size());
}

void BackupEntriesCollector::tryGatherMetadataAndCompareWithPrevious(int attempt_no, std::optional<Exception> & inconsistency_error, bool & need_another_attempt)
{
    try
    {
        /// Collect information about databases and tables specified in the BACKUP query.
        database_infos.clear();
        table_infos.clear();
        gatherDatabasesMetadata();
        gatherTablesMetadata();
        lockTablesForReading();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP)
            throw;

        /// If gatherDatabasesMetadata() or gatherTablesMetadata() threw a INCONSISTENT_METADATA_FOR_BACKUP error then the metadata is not consistent by itself,
        /// for example a CREATE QUERY could contain a wrong table name if the table has been just renamed. We need another attempt in that case.
        inconsistency_error = e;
        need_another_attempt = true;
        return;
    }

    if (!compare_collected_metadata)
        return;

    /// We have to check consistency of collected information to protect from the case when some table or database is
    /// renamed during this collecting making the collected information invalid.
    String mismatch_description;
    if (!compareWithPrevious(mismatch_description))
    {
        /// Usually two passes is minimum.
        /// (Because we need to compare with table names from the previous pass to be sure we are not going to miss anything).
        if (attempt_no >= 1)
            inconsistency_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "{}", mismatch_description};
        need_another_attempt = true;
    }
}

void BackupEntriesCollector::syncMetadataGatheringWithOtherHosts(int attempt_no, std::optional<Exception> & inconsistency_error, bool & need_another_attempt)
{
    String next_stage = Stage::formatGatheringMetadata(attempt_no + 1);

    if (inconsistency_error)
    {
        /// Failed to gather metadata or something wasn't consistent. We'll let other hosts know that and try again.
        LOG_WARNING(log, "Found inconsistency: {}", inconsistency_error->displayText());
        setStage(next_stage, inconsistency_error->displayText());
        return;
    }

    /// We've gathered metadata, cool! But we have to check that other hosts coped with that too.
    auto all_hosts_results = setStage(next_stage, need_another_attempt ? "need_another_attempt" : "consistent");
    size_t count = std::min(all_hosts.size(), all_hosts_results.size());

    for (size_t i = 0; i != count; ++i)
    {
        const auto & other_host = all_hosts[i];
        const auto & other_host_result = all_hosts_results[i];
        if ((other_host_result != "consistent") && (other_host_result != "need_another_attempt"))
        {
            LOG_WARNING(log, "Found inconsistency on host {}: {}", other_host, other_host_result);
            inconsistency_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Found inconsistency on host {}: {}", other_host, other_host_result};
            need_another_attempt = true;
            return;
        }
    }

    if (need_another_attempt)
    {
        LOG_TRACE(log, "Needs another attempt of collecting metadata");
        return;
    }

    for (size_t i = 0; i != count; ++i)
    {
        const auto & other_host = all_hosts[i];
        const auto & other_host_result = all_hosts_results[i];
        if (other_host_result == "need_another_attempt")
        {
            LOG_TRACE(log, "Host {} needs another attempt of collecting metadata", other_host);
            need_another_attempt = true;
            return;
        }
    }
}

void BackupEntriesCollector::gatherDatabasesMetadata()
{
    /// Collect information about databases and tables specified in the BACKUP query.
    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::ElementType::TABLE:
            {
                gatherDatabaseMetadata(
                    element.database_name,
                    /* throw_if_database_not_found= */ true,
                    /* backup_create_database_query= */ false,
                    element.table_name,
                    /* throw_if_table_not_found= */ true,
                    element.partitions,
                    /* all_tables= */ false,
                    /* except_table_names= */ {});
                break;
            }

            case ASTBackupQuery::ElementType::TEMPORARY_TABLE:
            {
                gatherDatabaseMetadata(
                    DatabaseCatalog::TEMPORARY_DATABASE,
                    /* throw_if_database_not_found= */ true,
                    /* backup_create_database_query= */ false,
                    element.table_name,
                    /* throw_if_table_not_found= */ true,
                    element.partitions,
                    /* all_tables= */ false,
                    /* except_table_names= */ {});
                break;
            }

            case ASTBackupQuery::ElementType::DATABASE:
            {
                gatherDatabaseMetadata(
                    element.database_name,
                    /* throw_if_database_not_found= */ true,
                    /* backup_create_database_query= */ true,
                    /* table_name= */ {},
                    /* throw_if_table_not_found= */ false,
                    /* partitions= */ {},
                    /* all_tables= */ true,
                    /* except_table_names= */ element.except_tables);
                break;
            }

            case ASTBackupQuery::ElementType::ALL:
            {
                for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
                {
                    if (!element.except_databases.contains(database_name))
                    {
                        gatherDatabaseMetadata(
                            database_name,
                            /* throw_if_database_not_found= */ false,
                            /* backup_create_database_query= */ true,
                            /* table_name= */ {},
                            /* throw_if_table_not_found= */ false,
                            /* partitions= */ {},
                            /* all_tables= */ true,
                            /* except_table_names= */ element.except_tables);
                    }
                }
                break;
            }
        }
    }
}

void BackupEntriesCollector::gatherDatabaseMetadata(
    const String & database_name,
    bool throw_if_database_not_found,
    bool backup_create_database_query,
    const std::optional<String> & table_name,
    bool throw_if_table_not_found,
    const std::optional<ASTs> & partitions,
    bool all_tables,
    const std::set<DatabaseAndTableName> & except_table_names)
{
    checkIsQueryCancelled();

    auto it = database_infos.find(database_name);
    if (it == database_infos.end())
    {
        DatabasePtr database;
        if (throw_if_database_not_found)
        {
            database = DatabaseCatalog::instance().getDatabase(database_name);
        }
        else
        {
            database = DatabaseCatalog::instance().tryGetDatabase(database_name);
            if (!database)
                return;
        }

        DatabaseInfo new_database_info;
        new_database_info.database = database;
        it = database_infos.emplace(database_name, new_database_info).first;
    }

    DatabaseInfo & database_info = it->second;

    if (backup_create_database_query && !database_info.create_database_query && (database_name != DatabaseCatalog::TEMPORARY_DATABASE))
    {
        ASTPtr create_database_query;
        try
        {
            create_database_query = database_info.database->getCreateDatabaseQuery();
        }
        catch (...)
        {
            /// Probably the database has been just removed.
            if (throw_if_database_not_found)
                throw;
            LOG_WARNING(log, "Couldn't get a create query for database {}", backQuoteIfNeed(database_name));
            return;
        }

        auto * create = create_database_query->as<ASTCreateQuery>();
        if (create->getDatabase() != database_name)
        {
            /// Probably the database has been just renamed. Use the older name for backup to keep the backup consistent.
            LOG_WARNING(log, "Got a create query with unexpected name {} for database {}",
                        backQuoteIfNeed(create->getDatabase()), backQuoteIfNeed(database_name));
            create_database_query = create_database_query->clone();
            create = create_database_query->as<ASTCreateQuery>();
            create->setDatabase(database_name);
        }

        database_info.create_database_query = create_database_query;
        String new_database_name = renaming_map.getNewDatabaseName(database_name);
        database_info.metadata_path_in_backup = root_path_in_backup / "metadata" / (escapeForFileName(new_database_name) + ".sql");
    }

    if (table_name)
    {
        auto & table_params = database_info.tables[*table_name];
        if (throw_if_table_not_found)
            table_params.throw_if_table_not_found = true;
        if (partitions)
        {
            table_params.partitions.emplace();
            insertAtEnd(*table_params.partitions, *partitions);
        }
        database_info.except_table_names.emplace(*table_name);
    }

    if (all_tables)
    {
        database_info.all_tables = all_tables;
        for (const auto & except_table_name : except_table_names)
            if (except_table_name.first == database_name)
                database_info.except_table_names.emplace(except_table_name.second);
    }
}

void BackupEntriesCollector::gatherTablesMetadata()
{
    checkIsQueryCancelled();

    table_infos.clear();
    for (const auto & [database_name, database_info] : database_infos)
    {
        std::vector<std::pair<ASTPtr, StoragePtr>> db_tables = findTablesInDatabase(database_name);

        for (const auto & db_table : db_tables)
        {
            const auto & create_table_query = db_table.first;
            const auto & storage = db_table.second;
            const auto & create = create_table_query->as<const ASTCreateQuery &>();
            String table_name = create.getTable();

            fs::path metadata_path_in_backup, data_path_in_backup;
            auto table_name_in_backup = renaming_map.getNewTableName({database_name, table_name});
            if (table_name_in_backup.database == DatabaseCatalog::TEMPORARY_DATABASE)
            {
                metadata_path_in_backup = root_path_in_backup / "temporary_tables" / "metadata" / (escapeForFileName(table_name_in_backup.table) + ".sql");
                data_path_in_backup = root_path_in_backup / "temporary_tables" / "data" / escapeForFileName(table_name_in_backup.table);
            }
            else
            {
                metadata_path_in_backup
                    = root_path_in_backup / "metadata" / escapeForFileName(table_name_in_backup.database) / (escapeForFileName(table_name_in_backup.table) + ".sql");
                data_path_in_backup = root_path_in_backup / "data" / escapeForFileName(table_name_in_backup.database)
                    / escapeForFileName(table_name_in_backup.table);
            }

            /// Add information to `table_infos`.
            auto & res_table_info = table_infos[QualifiedTableName{database_name, table_name}];
            res_table_info.database = database_info.database;
            res_table_info.storage = storage;
            res_table_info.create_table_query = create_table_query;
            res_table_info.metadata_path_in_backup = metadata_path_in_backup;
            res_table_info.data_path_in_backup = data_path_in_backup;

            if (!backup_settings.structure_only)
            {
                auto it = database_info.tables.find(table_name);
                if (it != database_info.tables.end())
                {
                    const auto & partitions = it->second.partitions;
                    if (partitions && storage && !storage->supportsBackupPartition())
                    {
                        throw Exception(
                            ErrorCodes::CANNOT_BACKUP_TABLE,
                            "Table engine {} doesn't support partitions, cannot backup {}",
                            storage->getName(),
                            tableNameWithTypeToString(database_name, table_name, false));
                    }
                    res_table_info.partitions = partitions;
                }
            }
        }
    }
}

std::vector<std::pair<ASTPtr, StoragePtr>> BackupEntriesCollector::findTablesInDatabase(const String & database_name) const
{
    const auto & database_info = database_infos.at(database_name);
    const auto & database = database_info.database;

    checkIsQueryCancelled();

    auto filter_by_table_name = [&](const String & table_name)
    {
        if (BackupUtils::isInnerTable(database_name, table_name))
            return false;

        if (database_info.tables.contains(table_name))
            return true;

        if (database_info.all_tables)
            return !database_info.except_table_names.contains(table_name);

        return false;
    };

    std::vector<std::pair<ASTPtr, StoragePtr>> db_tables;

    try
    {
        /// Database or table could be replicated - so may use ZooKeeper. We need to retry.
        auto zookeeper_retries_info = global_zookeeper_retries_info;
        ZooKeeperRetriesControl retries_ctl("getTablesForBackup", log, zookeeper_retries_info, nullptr);
        retries_ctl.retryLoop([&](){ db_tables = database->getTablesForBackup(filter_by_table_name, context); });
    }
    catch (Exception & e)
    {
        e.addMessage("While collecting tables for backup in database {}", backQuoteIfNeed(database_name));
        throw;
    }

    std::unordered_set<String> found_table_names;
    for (auto & db_table : db_tables)
    {
        auto create_table_query = db_table.first;
        auto * create = create_table_query->as<ASTCreateQuery>();
        found_table_names.emplace(create->getTable());

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
        {
            if (!create->temporary)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Got a non-temporary create query for {}",
                                tableNameWithTypeToString(database_name, create->getTable(), false));
            }
        }
        else
        {
            if (create->getDatabase() != database_name)
            {
                /// Probably the table has been just renamed. Use the older name for backup to keep the backup consistent.
                LOG_WARNING(log, "Got a create query with unexpected database name {} for {}",
                            backQuoteIfNeed(create->getDatabase()),
                            tableNameWithTypeToString(database_name, create->getTable(), false));
                create_table_query = create_table_query->clone();
                create = create_table_query->as<ASTCreateQuery>();
                create->setDatabase(database_name);
                db_table.first = create_table_query;
            }
        }
    }

    /// Check that all tables were found.
    for (const auto & [table_name, table_info] : database_info.tables)
    {
        if (table_info.throw_if_table_not_found && !found_table_names.contains(table_name))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "{} was not found", tableNameWithTypeToString(database_name, table_name, true));
    }

    return db_tables;
}

void BackupEntriesCollector::lockTablesForReading()
{
    for (auto & [table_name, table_info] : table_infos)
    {
        auto storage = table_info.storage;
        if (!storage)
            continue;

        checkIsQueryCancelled();

        table_info.table_lock = storage->tryLockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    }

    std::erase_if(
        table_infos,
        [](const std::pair<QualifiedTableName, TableInfo> & key_value)
        {
            const auto & table_info = key_value.second;
            return table_info.storage && !table_info.table_lock; /// Table was dropped while acquiring the lock.
        });
}

/// Check consistency of collected information about databases and tables.
bool BackupEntriesCollector::compareWithPrevious(String & mismatch_description)
{
    /// We need to scan tables at least twice to be sure that we haven't missed any table which could be renamed
    /// while we were scanning.
    std::vector<std::pair<String, String>> databases_metadata;
    std::vector<std::pair<QualifiedTableName, String>> tables_metadata;

    databases_metadata.reserve(database_infos.size());
    tables_metadata.reserve(table_infos.size());

    for (const auto & [database_name, database_info] : database_infos)
        databases_metadata.emplace_back(database_name, database_info.create_database_query ? serializeAST(*database_info.create_database_query) : "");
    for (const auto & [table_name, table_info] : table_infos)
        tables_metadata.emplace_back(table_name, serializeAST(*table_info.create_table_query));

    /// We need to sort the lists to make the comparison below correct.
    ::sort(databases_metadata.begin(), databases_metadata.end());
    ::sort(tables_metadata.begin(), tables_metadata.end());

    SCOPE_EXIT({
        previous_databases_metadata = std::move(databases_metadata);
        previous_tables_metadata = std::move(tables_metadata);
    });

    enum class MismatchType : uint8_t
    {
        ADDED,
        REMOVED,
        CHANGED,
        NONE
    };

    /// Helper function - used to compare the metadata of databases and tables.
    auto find_mismatch = [](const auto & metadata, const auto & previous_metadata)
        -> std::pair<MismatchType, typename std::remove_cvref_t<decltype(metadata)>::value_type::first_type>
    {
        auto [mismatch, p_mismatch] = std::mismatch(metadata.begin(), metadata.end(), previous_metadata.begin(), previous_metadata.end());

        if ((mismatch != metadata.end()) && (p_mismatch != previous_metadata.end()))
        {
            if (mismatch->first == p_mismatch->first)
                return {MismatchType::CHANGED, mismatch->first};
            if (mismatch->first < p_mismatch->first)
                return {MismatchType::ADDED, mismatch->first};
            return {MismatchType::REMOVED, mismatch->first};
        }
        if (mismatch != metadata.end())
        {
            return {MismatchType::ADDED, mismatch->first};
        }
        if (p_mismatch != previous_metadata.end())
        {
            return {MismatchType::REMOVED, p_mismatch->first};
        }

        return {MismatchType::NONE, {}};
    };

    /// Databases must be the same as during the previous scan.
    if (auto mismatch = find_mismatch(databases_metadata, previous_databases_metadata); mismatch.first != MismatchType::NONE)
    {
        if (mismatch.first == MismatchType::ADDED)
            mismatch_description = fmt::format("Database {} was added during scanning", backQuoteIfNeed(mismatch.second));
        else if (mismatch.first == MismatchType::REMOVED)
            mismatch_description = fmt::format("Database {} was removed during scanning", backQuoteIfNeed(mismatch.second));
        else
            mismatch_description = fmt::format("Database {} changed its definition during scanning", backQuoteIfNeed(mismatch.second));
        return false;
    }

    /// Tables must be the same as during the previous scan.
    if (auto mismatch = find_mismatch(tables_metadata, previous_tables_metadata); mismatch.first != MismatchType::NONE)
    {
        if (mismatch.first == MismatchType::ADDED)
            mismatch_description = fmt::format("{} was added during scanning", tableNameWithTypeToString(mismatch.second, true));
        else if (mismatch.first == MismatchType::REMOVED)
            mismatch_description = fmt::format("{} was removed during scanning", tableNameWithTypeToString(mismatch.second, true));
        else
            mismatch_description = fmt::format("{} changed its definition during scanning", tableNameWithTypeToString(mismatch.second, true));
        return false;
    }

    return true;
}

/// Make backup entries for all the definitions of all the databases found.
void BackupEntriesCollector::makeBackupEntriesForDatabasesDefs()
{
    for (const auto & [database_name, database_info] : database_infos)
    {
        if (!database_info.create_database_query)
            continue; /// We store CREATE DATABASE queries only if there was BACKUP DATABASE specified.

        LOG_TRACE(log, "Adding the definition of database {} to backup", backQuoteIfNeed(database_name));
        checkIsQueryCancelled();

        ASTPtr new_create_query = database_info.create_database_query;
        adjustCreateQueryForBackup(new_create_query, context->getGlobalContext());
        renameDatabaseAndTableNameInCreateQuery(new_create_query, renaming_map, context->getGlobalContext());

        const String & metadata_path_in_backup = database_info.metadata_path_in_backup;
        backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*new_create_query)));
    }
}

/// Calls IDatabase::backupTable() for all the tables found to make backup entries for tables.
void BackupEntriesCollector::makeBackupEntriesForTablesDefs()
{
    for (auto & [table_name, table_info] : table_infos)
    {
        LOG_TRACE(log, "Adding the definition of {} to backup", tableNameWithTypeToString(table_name.database, table_name.table, false));
        checkIsQueryCancelled();

        ASTPtr new_create_query = table_info.create_table_query;
        table_info.replicated_table_zk_path = extractZooKeeperPathFromReplicatedTableDef(new_create_query->as<const ASTCreateQuery &>(), context);
        adjustCreateQueryForBackup(new_create_query, context->getGlobalContext());
        renameDatabaseAndTableNameInCreateQuery(new_create_query, renaming_map, context->getGlobalContext());

        const String & metadata_path_in_backup = table_info.metadata_path_in_backup;
        backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*new_create_query)));
    }
}

void BackupEntriesCollector::makeBackupEntriesForTablesData()
{
    if (backup_settings.structure_only)
        return;

    ThreadPoolCallbackRunnerLocal<void> runner(threadpool, "BackupCollect");
    for (const auto & table_name : table_infos | boost::adaptors::map_keys)
    {
        runner([&]()
        {
            makeBackupEntriesForTableData(table_name);
        });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
}

void BackupEntriesCollector::makeBackupEntriesForTableData(const QualifiedTableName & table_name)
{
    if (backup_settings.structure_only)
        return;

    const auto & table_info = table_infos.at(table_name);
    const auto & storage = table_info.storage;
    const auto & data_path_in_backup = table_info.data_path_in_backup;

    if (!storage)
    {
        /// If storage == null that means this storage exists on other replicas but it has not been created on this replica yet.
        /// If this table is replicated in this case we call IBackupCoordination::addReplicatedDataPath() which will cause
        /// other replicas to fill the storage's data in the backup.
        /// If this table is not replicated we'll do nothing leaving the storage's data empty in the backup.
        if (table_info.replicated_table_zk_path)
            backup_coordination->addReplicatedDataPath(*table_info.replicated_table_zk_path, data_path_in_backup);
        return;
    }

    LOG_TRACE(log, "Collecting data of {} for backup", tableNameWithTypeToString(table_name.database, table_name.table, false));
    checkIsQueryCancelled();

    try
    {
        storage->backupData(*this, data_path_in_backup, table_info.partitions);
    }
    catch (Exception & e)
    {
        e.addMessage("While collecting data of {} for backup", tableNameWithTypeToString(table_name.database, table_name.table, false));
        throw;
    }
}

void BackupEntriesCollector::addBackupEntryUnlocked(const String & file_name, BackupEntryPtr backup_entry)
{
    if (current_stage == Stage::WRITING_BACKUP)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding backup entries is not allowed");
    backup_entries.emplace_back(file_name, backup_entry);
}

void BackupEntriesCollector::addBackupEntry(const String & file_name, BackupEntryPtr backup_entry)
{
    std::lock_guard lock(mutex);
    addBackupEntryUnlocked(file_name, backup_entry);
}

void BackupEntriesCollector::addBackupEntry(const std::pair<String, BackupEntryPtr> & backup_entry)
{
    std::lock_guard lock(mutex);
    addBackupEntryUnlocked(backup_entry.first, backup_entry.second);
}

void BackupEntriesCollector::addBackupEntries(const BackupEntries & backup_entries_)
{
    std::lock_guard lock(mutex);
    if (current_stage == Stage::WRITING_BACKUP)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding of backup entries is not allowed");
    insertAtEnd(backup_entries, backup_entries_);
}

void BackupEntriesCollector::addBackupEntries(BackupEntries && backup_entries_)
{
    std::lock_guard lock(mutex);
    if (current_stage == Stage::WRITING_BACKUP)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding of backup entries is not allowed");
    insertAtEnd(backup_entries, std::move(backup_entries_));
}

void BackupEntriesCollector::addPostTask(std::function<void()> task)
{
    std::lock_guard lock(mutex);
    if (current_stage == Stage::WRITING_BACKUP)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding of post tasks is not allowed");
    post_tasks.push(std::move(task));
}

/// Runs all the tasks added with addPostCollectingTask().
void BackupEntriesCollector::runPostTasks()
{
    LOG_TRACE(log, "Will run {} post tasks", post_tasks.size());

    /// Post collecting tasks can add other post collecting tasks, our code is fine with that.
    while (!post_tasks.empty())
    {
        checkIsQueryCancelled();

        auto task = std::move(post_tasks.front());
        post_tasks.pop();
        std::move(task)();
    }

    LOG_TRACE(log, "All post tasks successfully executed");
}

std::unordered_map<UUID, AccessEntityPtr> BackupEntriesCollector::getAllAccessEntities()
{
    std::lock_guard lock(mutex);
    if (!all_access_entities)
    {
        all_access_entities.emplace();
        auto entities_with_ids = context->getAccessControl().readAllWithIDs();
        for (const auto & [id, entity] : entities_with_ids)
        {
            if (entity->isBackupAllowed())
                all_access_entities->emplace(id, entity);
        }
    }
    return *all_access_entities;
}

}
