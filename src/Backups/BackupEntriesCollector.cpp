#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackupCoordination.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <base/chrono_io.h>
#include <base/insertAtEnd.h>
#include <Common/escapeForFileName.h>
#include <boost/range/algorithm/copy.hpp>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COLLECT_OBJECTS_FOR_BACKUP;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int BACKUP_IS_EMPTY;
    extern const int TABLE_IS_DROPPED;
}


BackupEntriesCollector::BackupEntriesCollector(
    const ASTBackupQuery::Elements & backup_query_elements_,
    const BackupSettings & backup_settings_,
    std::shared_ptr<IBackupCoordination> backup_coordination_,
    const ContextPtr & context_,
    std::chrono::seconds timeout_)
    : backup_query_elements(backup_query_elements_)
    , backup_settings(backup_settings_)
    , backup_coordination(backup_coordination_)
    , context(context_)
    , timeout(timeout_)
    , log(&Poco::Logger::get("BackupEntriesCollector"))
{
}

BackupEntriesCollector::~BackupEntriesCollector() = default;

BackupEntries BackupEntriesCollector::getBackupEntries()
{
    try
    {
        /// getBackupEntries() must not be called multiple times.
        if (current_stage != Stage::kPreparing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Already making backup entries");

        /// Calculate the root path for collecting backup entries, it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
        calculateRootPathInBackup();

        /// Do renaming in the create queries according to the renaming config.
        renaming_settings.setFromBackupQuery(backup_query_elements);

        /// Find databases and tables which we're going to put to the backup.
        setStage(Stage::kFindingTables);
        collectDatabasesAndTablesInfo();

        /// Make backup entries for the definitions of the found databases.
        makeBackupEntriesForDatabasesDefs();

        /// Make backup entries for the definitions of the found tables.
        makeBackupEntriesForTablesDefs();

        /// Make backup entries for the data of the found tables.
        setStage(Stage::kExtractingDataFromTables);
        makeBackupEntriesForTablesData();

        /// Run all the tasks added with addPostCollectingTask().
        setStage(Stage::kRunningPostTasks);
        runPostCollectingTasks();

        /// No more backup entries or tasks are allowed after this point.
        setStage(Stage::kWritingBackup);

        return std::move(backup_entries);
    }
    catch (...)
    {
        try
        {
            setStage(Stage::kError, getCurrentExceptionMessage(false));
        }
        catch (...)
        {
        }
        throw;
    }
}

void BackupEntriesCollector::setStage(Stage new_stage, const String & error_message)
{
    if (new_stage == Stage::kError)
        LOG_ERROR(log, "{} failed with error: {}", toString(current_stage), error_message);
    else
        LOG_TRACE(log, "{}", toString(new_stage));
    
    current_stage = new_stage;
    
    if (new_stage == Stage::kError)
    {
        backup_coordination->syncStageError(backup_settings.host_id, error_message);
    }
    else
    {
        auto all_hosts
            = BackupSettings::Util::filterHostIDs(backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num);
        backup_coordination->syncStage(backup_settings.host_id, static_cast<int>(new_stage), all_hosts, timeout);
    }
}

std::string_view BackupEntriesCollector::toString(Stage stage)
{
    switch (stage)
    {
        case Stage::kPreparing: return "Preparing";
        case Stage::kFindingTables: return "Finding tables";
        case Stage::kExtractingDataFromTables: return "Extracting data from tables";
        case Stage::kRunningPostTasks: return "Running post tasks";
        case Stage::kWritingBackup: return "Writing backup";
        case Stage::kError: return "Error";
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown backup stage: {}", static_cast<int>(stage));
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
void BackupEntriesCollector::collectDatabasesAndTablesInfo()
{
    bool use_timeout = (timeout.count() >= 0);
    auto start_time = std::chrono::steady_clock::now();

    int pass = 0;
    do
    {
        database_infos.clear();
        table_infos.clear();
        consistent = true;

        /// Collect information about databases and tables specified in the BACKUP query.
        for (const auto & element : backup_query_elements)
        {
            switch (element.type)
            {
                case ASTBackupQuery::ElementType::TABLE:
                {
                    collectTableInfo(element.name, element.partitions, true);
                    break;
                }

                case ASTBackupQuery::ElementType::DATABASE:
                {
                    collectDatabaseInfo(element.name.first, element.except_list, true);
                    break;
                }

                case ASTBackupQuery::ElementType::ALL_DATABASES:
                {
                    collectAllDatabasesInfo(element.except_list);
                    break;
                }
            }
        }

        /// We have to check consistency of collected information to protect from the case when some table or database is
        /// renamed during this collecting making the collected information invalid.
        checkConsistency();

        /// Two passes is absolute minimum (see `previous_table_names` & `previous_database_names`).
        if (!consistent && (pass >= 2) && use_timeout)
        {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > timeout)
                throw Exception(
                    ErrorCodes::CANNOT_COLLECT_OBJECTS_FOR_BACKUP,
                    "Couldn't collect tables and databases to make a backup (pass #{}, elapsed {})",
                    pass,
                    to_string(elapsed));
        }

        ++pass;
    } while (!consistent);

    LOG_INFO(log, "Will backup {} databases and {} tables", database_infos.size(), table_infos.size());
}

void BackupEntriesCollector::collectTableInfo(
    const DatabaseAndTableName & table_name, const std::optional<ASTs> & partitions, bool throw_if_not_found)
{
    /// Gather information about the table.
    DatabasePtr database;
    StoragePtr storage;
    TableLockHolder table_lock;
    ASTPtr create_table_query;

    if (throw_if_not_found)
    {
        std::tie(database, storage)
            = DatabaseCatalog::instance().getDatabaseAndTable(StorageID{table_name.first, table_name.second}, context);
        table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
        create_table_query = database->getCreateTableQuery(table_name.second, context);
    }
    else
    {
        std::tie(database, storage)
            = DatabaseCatalog::instance().tryGetDatabaseAndTable(StorageID{table_name.first, table_name.second}, context);
        if (!storage)
        {
            consistent &= !table_infos.contains(table_name);
            return;
        }

        try
        {
            table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            {
                consistent &= !table_infos.contains(table_name);
                return;
            }
            throw;
        }

        create_table_query = database->tryGetCreateTableQuery(table_name.second, context);
        if (!create_table_query)
        {
            consistent &= !table_infos.contains(table_name);
            return;
        }
    }

    storage->adjustCreateQueryForBackup(create_table_query);
    auto new_table_name = renaming_settings.getNewTableName(table_name);
    fs::path data_path_in_backup
        = root_path_in_backup / "data" / escapeForFileName(new_table_name.first) / escapeForFileName(new_table_name.second);

    /// Check that information is consistent.
    const auto & create = create_table_query->as<const ASTCreateQuery &>();
    if ((create.getDatabase() != table_name.first) || (create.getTable() != table_name.second))
    {
        /// Table was renamed recently.
        consistent = false;
        return;
    }

    if (auto it = table_infos.find(table_name); it != table_infos.end())
    {
        const auto & table_info = it->second;
        if ((table_info.database != database) || (table_info.storage != storage))
        {
            /// Table was renamed recently.
            consistent = false;
            return;
        }
    }

    /// Add information to `table_infos`.
    auto & res_table_info = table_infos[table_name];
    res_table_info.database = database;
    res_table_info.storage = storage;
    res_table_info.table_lock = table_lock;
    res_table_info.create_table_query = create_table_query;
    res_table_info.data_path_in_backup = data_path_in_backup;

    if (partitions)
    {
        if (!res_table_info.partitions)
            res_table_info.partitions.emplace();
        insertAtEnd(*res_table_info.partitions, *partitions);
    }
}

void BackupEntriesCollector::collectDatabaseInfo(const String & database_name, const std::set<String> & except_table_names, bool throw_if_not_found)
{
    /// Gather information about the database.
    DatabasePtr database;
    ASTPtr create_database_query;

    if (throw_if_not_found)
    {
        database = DatabaseCatalog::instance().getDatabase(database_name);
        create_database_query = database->getCreateDatabaseQuery();
    }
    else
    {
        database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
        {
            consistent &= !database_infos.contains(database_name);
            return;
        }

        try
        {
            create_database_query = database->getCreateDatabaseQuery();
        }
        catch (...)
        {
            /// The database has been dropped recently.
            consistent &= !database_infos.contains(database_name);
            return;
        }
    }

    /// Check that information is consistent.
    const auto & create = create_database_query->as<const ASTCreateQuery &>();
    if (create.getDatabase() != database_name)
    {
        /// Database was renamed recently.
        consistent = false;
        return;
    }

    if (auto it = database_infos.find(database_name); it != database_infos.end())
    {
        const auto & database_info = it->second;
        if (database_info.database != database)
        {
            /// Database was renamed recently.
            consistent = false;
            return;
        }
    }

    /// Add information to `database_infos`.
    auto & res_database_info = database_infos[database_name];
    res_database_info.database = database;
    res_database_info.create_database_query = create_database_query;

    for (auto it = database->getTablesIteratorForBackup(*this); it->isValid(); it->next())
    {
        if (except_table_names.contains(it->name()))
            continue;

        collectTableInfo(DatabaseAndTableName{database_name, it->name()}, {}, false);
        if (!consistent)
            return;
    }
}

void BackupEntriesCollector::collectAllDatabasesInfo(const std::set<String> & except_database_names)
{
    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabasesForBackup())
    {
        if (except_database_names.contains(database_name))
            continue;
        collectDatabaseInfo(database_name, {}, false);
        if (!consistent)
            return;
    }
}

/// Check for consistency of collected information about databases and tables.
void BackupEntriesCollector::checkConsistency()
{
    if (!consistent)
        return; /// Already inconsistent, no more checks necessary

    /// Databases found while we were scanning tables and while we were scanning databases - must be the same.
    for (const auto & [table_name, table_info] : table_infos)
    {
        auto it = database_infos.find(table_name.first);
        if (it != database_infos.end())
        {
            const auto & database_info = it->second;
            if (database_info.database != table_info.database)
            {
                consistent = false;
                return;
            }
        }
    }

    /// We need to scan tables at least twice to be sure that we haven't missed any table which could be renamed
    /// while we were scanning.
    std::set<String> database_names;
    std::set<DatabaseAndTableName> table_names;
    boost::range::copy(database_infos | boost::adaptors::map_keys, std::inserter(database_names, database_names.end()));
    boost::range::copy(table_infos | boost::adaptors::map_keys, std::inserter(table_names, table_names.end()));

    if (!previous_database_names || !previous_table_names || (*previous_database_names != database_names)
        || (*previous_table_names != table_names))
    {
        previous_database_names = std::move(database_names);
        previous_table_names = std::move(table_names);
        consistent = false;
    }
}

/// Make backup entries for all the definitions of all the databases found.
void BackupEntriesCollector::makeBackupEntriesForDatabasesDefs()
{
    for (const auto & [database_name, database_info] : database_infos)
    {
        LOG_TRACE(log, "Adding definition of database {}", backQuoteIfNeed(database_name));

        ASTPtr new_create_query = database_info.create_database_query;
        renameInCreateQuery(new_create_query, renaming_settings, context);

        String new_database_name = renaming_settings.getNewDatabaseName(database_name);
        auto metadata_path_in_backup = root_path_in_backup / "metadata" / (escapeForFileName(new_database_name) + ".sql");

        backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*new_create_query)));
    }
}

/// Calls IDatabase::backupTable() for all the tables found to make backup entries for tables.
void BackupEntriesCollector::makeBackupEntriesForTablesDefs()
{
    for (const auto & [table_name, table_info] : table_infos)
    {
        LOG_TRACE(log, "Adding definition of table {}.{}", backQuoteIfNeed(table_name.first), backQuoteIfNeed(table_name.second));
        const auto & database = table_info.database;
        const auto & storage = table_info.storage;
        database->backupCreateTableQuery(*this, storage, table_info.create_table_query);
    }
}

void BackupEntriesCollector::makeBackupEntriesForTablesData()
{
    if (backup_settings.structure_only)
        return;

    for (const auto & [table_name, table_info] : table_infos)
    {
        LOG_TRACE(log, "Adding data of table {}.{}", backQuoteIfNeed(table_name.first), backQuoteIfNeed(table_name.second));
        const auto & storage = table_info.storage;
        const auto & data_path_in_backup = table_info.data_path_in_backup;
        const auto & partitions = table_info.partitions;
        storage->backupData(*this, data_path_in_backup, partitions);
    }
}

void BackupEntriesCollector::addBackupEntry(const String & file_name, BackupEntryPtr backup_entry)
{
    if (current_stage == Stage::kWritingBackup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding backup entries is not allowed");
    backup_entries.emplace_back(file_name, backup_entry);
}

void BackupEntriesCollector::addBackupEntries(const BackupEntries & backup_entries_)
{
    if (current_stage == Stage::kWritingBackup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding backup entries is not allowed");
    insertAtEnd(backup_entries, backup_entries_);
}

void BackupEntriesCollector::addBackupEntries(BackupEntries && backup_entries_)
{
    if (current_stage == Stage::kWritingBackup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding backup entries is not allowed");
    insertAtEnd(backup_entries, std::move(backup_entries_));
}

void BackupEntriesCollector::addBackupEntryForCreateQuery(const ASTPtr & create_query)
{
    ASTPtr new_create_query = create_query;
    renameInCreateQuery(new_create_query, renaming_settings, context);

    const auto & create = new_create_query->as<const ASTCreateQuery &>();
    String new_table_name = create.getTable();
    String new_database_name = create.getDatabase();
    auto metadata_path_in_backup
        = root_path_in_backup / "metadata" / escapeForFileName(new_database_name) / (escapeForFileName(new_table_name) + ".sql");

    addBackupEntry(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*create_query)));
}

void BackupEntriesCollector::addPostCollectingTask(std::function<void()> task)
{
    if (current_stage == Stage::kWritingBackup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding post tasks is not allowed");
    post_collecting_tasks.push(std::move(task));
}

/// Runs all the tasks added with addPostCollectingTask().
void BackupEntriesCollector::runPostCollectingTasks()
{
    /// Post collecting tasks can add other post collecting tasks, our code is fine with that.
    while (!post_collecting_tasks.empty())
    {
        auto task = std::move(post_collecting_tasks.front());
        post_collecting_tasks.pop();
        std::move(task)();
    }
}

void BackupEntriesCollector::throwPartitionsNotSupported(const StorageID & storage_id, const String & table_engine)
{
    throw Exception(
        ErrorCodes::CANNOT_BACKUP_TABLE,
        "Table engine {} doesn't support partitions, cannot backup table {}",
        table_engine,
        storage_id.getFullTableName());
}

}
