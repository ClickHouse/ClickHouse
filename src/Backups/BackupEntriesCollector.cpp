#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/BackupUtils.h>
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
    extern const int INCONSISTENT_METADATA_FOR_BACKUP;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int TABLE_IS_DROPPED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    String tableNameWithTypeToString(const String & database_name, const String & table_name, bool first_char_uppercase)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            return fmt::format("{}emporary table {}", first_char_uppercase ? 'T' : 't', backQuoteIfNeed(table_name));
        else
            return fmt::format("{}able {}.{}", first_char_uppercase ? 'T' : 't', backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
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
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown backup stage: {}", static_cast<int>(stage));
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
        renaming_map = makeRenamingMapFromBackupQuery(backup_query_elements);

        /// Find databases and tables which we're going to put to the backup.
        setStage(Stage::kFindingTables);
        gatherMetadataAndCheckConsistency();

        /// Make backup entries for the definitions of the found databases.
        makeBackupEntriesForDatabasesDefs();

        /// Make backup entries for the definitions of the found tables.
        makeBackupEntriesForTablesDefs();

        /// Make backup entries for the data of the found tables.
        setStage(Stage::kExtractingDataFromTables);
        makeBackupEntriesForTablesData();

        /// Run all the tasks added with addPostCollectingTask().
        setStage(Stage::kRunningPostTasks);
        runPostTasks();

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
    bool use_timeout = (timeout.count() >= 0);
    auto start_time = std::chrono::steady_clock::now();

    for (size_t pass = 1;; ++pass)
    {
        try
        {
            /// Collect information about databases and tables specified in the BACKUP query.
            database_infos.clear();
            table_infos.clear();
            gatherDatabasesMetadata();
            gatherTablesMetadata();

            /// We have to check consistency of collected information to protect from the case when some table or database is
            /// renamed during this collecting making the collected information invalid.
            auto comparing_error = compareWithPrevious();
            if (!comparing_error)
                break; /// no error, everything's fine
            
            if (pass >= 2) /// Two passes is minimum (we need to compare with table names with previous ones to be sure we don't miss anything).
                throw *comparing_error;
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP)
                throw;

            auto elapsed = std::chrono::steady_clock::now() - start_time;
            e.addMessage("Couldn't gather tables and databases to make a backup (pass #{}, elapsed {})", pass, to_string(elapsed));
            if (use_timeout && (elapsed > timeout))
                throw;
            else
                LOG_WARNING(log, "{}", e.displayText());
        }
    }

    LOG_INFO(log, "Will backup {} databases and {} tables", database_infos.size(), table_infos.size());
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

    if (backup_create_database_query && !database_info.create_database_query && !DatabaseCatalog::isPredefinedDatabaseName(database_name))
    {
        ASTPtr create_database_query;
        try
        {
            create_database_query = database_info.database->getCreateDatabaseQueryForBackup();
        }
        catch (...)
        {
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Couldn't get a create query for database {}", database_name);
        }
        
        database_info.create_database_query = create_database_query;
        const auto & create = create_database_query->as<const ASTCreateQuery &>();

        if (create.getDatabase() != database_name)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Got a create query with unexpected name {} for database {}", backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(database_name));

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
    table_infos.clear();
    for (const auto & [database_name, database_info] : database_infos)
    {
        const auto & database = database_info.database;
        bool is_temporary_database = (database_name == DatabaseCatalog::TEMPORARY_DATABASE);
        
        auto filter_by_table_name = [database_info = &database_info](const String & table_name)
        {
            /// We skip inner tables of materialized views.
            if (table_name.starts_with(".inner_id."))
                return false;
                    
            if (database_info->tables.contains(table_name))
                return true;

            if (database_info->all_tables)
                return !database_info->except_table_names.contains(table_name);

            return false;
        };

        auto db_tables = database->getTablesForBackup(filter_by_table_name, context);

        std::unordered_set<String> found_table_names;
        for (const auto & db_table : db_tables)
        {
            const auto & create_table_query = db_table.first;
            const auto & create = create_table_query->as<const ASTCreateQuery &>();
            found_table_names.emplace(create.getTable());

            if (is_temporary_database && !create.temporary)
                throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Got a non-temporary create query for {}", tableNameWithTypeToString(database_name, create.getTable(), false));
            
            if (!is_temporary_database && (create.getDatabase() != database_name))
                throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Got a create query with unexpected database name {} for {}", backQuoteIfNeed(create.getDatabase()), tableNameWithTypeToString(database_name, create.getTable(), false));
        }

        /// Check that all tables were found.
        for (const auto & [table_name, table_info] : database_info.tables)
        {
            if (table_info.throw_if_table_not_found && !found_table_names.contains(table_name))
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "{} not found", tableNameWithTypeToString(database_name, table_name, true));
        }

        for (const auto & db_table : db_tables)
        {
            const auto & create_table_query = db_table.first;
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
            res_table_info.database = database;
            res_table_info.storage = db_table.second;
            res_table_info.create_table_query = create_table_query;
            res_table_info.metadata_path_in_backup = metadata_path_in_backup;
            res_table_info.data_path_in_backup = data_path_in_backup;

            auto partitions_it = database_info.tables.find(table_name);
            if (partitions_it != database_info.tables.end())
                res_table_info.partitions = partitions_it->second.partitions;
        }
    }
}

void BackupEntriesCollector::lockTablesForReading()
{
    for (auto & [table_name, table_info] : table_infos)
    {
        auto storage = table_info.storage;
        TableLockHolder table_lock;
        if (storage)
        {
            try
            {
                table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                    throw;
                throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "{} is dropped", tableNameWithTypeToString(table_name.database, table_name.table, true));
            }
        }
    }
}

/// Check consistency of collected information about databases and tables.
std::optional<Exception> BackupEntriesCollector::compareWithPrevious()
{
    /// We need to scan tables at least twice to be sure that we haven't missed any table which could be renamed
    /// while we were scanning.
    std::set<String> database_names;
    std::set<QualifiedTableName> table_names;
    boost::range::copy(database_infos | boost::adaptors::map_keys, std::inserter(database_names, database_names.end()));
    boost::range::copy(table_infos | boost::adaptors::map_keys, std::inserter(table_names, table_names.end()));

    if (previous_database_names != database_names)
    {
        std::optional<Exception> comparing_error;
        for (const auto & database_name : database_names)
        {
            if (!previous_database_names.contains(database_name))
            {
                comparing_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Database {} were added during scanning", backQuoteIfNeed(database_name)};
                break;
            }
        }
        if (!comparing_error)
        {
            for (const auto & database_name : previous_database_names)
            {
                if (!database_names.contains(database_name))
                {
                    comparing_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Database {} were removed during scanning", backQuoteIfNeed(database_name)};
                    break;
                }
            }
        }
        assert(comparing_error);
        previous_database_names = std::move(database_names);
        previous_table_names = std::move(table_names);
        return comparing_error;
    }

    if (previous_table_names != table_names)
    {
        std::optional<Exception> comparing_error;
        for (const auto & table_name : table_names)
        {
            if (!previous_table_names.contains(table_name))
            {
                comparing_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "{} were added during scanning", tableNameWithTypeToString(table_name.database, table_name.table, true)};
                break;
            }
        }
        if (!comparing_error)
        {
            for (const auto & table_name : previous_table_names)
            {
                if (!table_names.contains(table_name))
                {
                    comparing_error = Exception{ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "{} were removed during scanning", tableNameWithTypeToString(table_name.database, table_name.table, true)};
                    break;
                }
            }
        }
        assert(comparing_error);
        previous_table_names = std::move(table_names);
        return comparing_error;
    }

    return {};
}

/// Make backup entries for all the definitions of all the databases found.
void BackupEntriesCollector::makeBackupEntriesForDatabasesDefs()
{
    for (const auto & [database_name, database_info] : database_infos)
    {
        if (!database_info.create_database_query)
            continue; /// We don't store CREATE queries for predefined databases (see DatabaseCatalog::isPredefinedDatabaseName()).
        
        LOG_TRACE(log, "Adding definition of database {}", backQuoteIfNeed(database_name));

        ASTPtr new_create_query = database_info.create_database_query;
        renameDatabaseAndTableNameInCreateQuery(context->getGlobalContext(), renaming_map, new_create_query);

        const String & metadata_path_in_backup = database_info.metadata_path_in_backup;
        backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*new_create_query)));
    }
}

/// Calls IDatabase::backupTable() for all the tables found to make backup entries for tables.
void BackupEntriesCollector::makeBackupEntriesForTablesDefs()
{
    for (const auto & [table_name, table_info] : table_infos)
    {
        LOG_TRACE(log, "Adding definition of {}", tableNameWithTypeToString(table_name.database, table_name.table, false));

        ASTPtr new_create_query = table_info.create_table_query;
        renameDatabaseAndTableNameInCreateQuery(context->getGlobalContext(), renaming_map, new_create_query);

        const String & metadata_path_in_backup = table_info.metadata_path_in_backup;
        backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*new_create_query)));
    }
}

void BackupEntriesCollector::makeBackupEntriesForTablesData()
{
    if (backup_settings.structure_only)
        return;

    for (const auto & [table_name, table_info] : table_infos)
    {
        const auto & storage = table_info.storage;
        if (!storage)
        {
            /// This storage exists on other replica and has not been created on this replica yet.
            /// We store metadata only for such tables.
            /// TODO: Need special processing if it's a ReplicatedMergeTree.
            continue;
        }

        LOG_TRACE(log, "Adding data of {}", tableNameWithTypeToString(table_name.database, table_name.table, false));
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

void BackupEntriesCollector::addPostTask(std::function<void()> task)
{
    if (current_stage == Stage::kWritingBackup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding post tasks is not allowed");
    post_tasks.push(std::move(task));
}

/// Runs all the tasks added with addPostCollectingTask().
void BackupEntriesCollector::runPostTasks()
{
    /// Post collecting tasks can add other post collecting tasks, our code is fine with that.
    while (!post_tasks.empty())
    {
        auto task = std::move(post_tasks.front());
        post_tasks.pop();
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
