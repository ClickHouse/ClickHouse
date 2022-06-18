#include <Backups/RestorerFromBackup.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupSettings.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupUtils.h>
#include <Access/AccessBackup.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Databases/IDatabase.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Storages/IStorage.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <base/insertAtEnd.h>
#include <boost/algorithm/string/join.hpp>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int CANNOT_RESTORE_DATABASE;
}


namespace
{
    constexpr const std::string_view sql_ext = ".sql";

    bool hasSystemTableEngine(const IAST & ast)
    {
        const ASTCreateQuery * create = ast.as<ASTCreateQuery>();
        if (!create)
            return false;
        if (!create->storage || !create->storage->engine)
            return false;
        return create->storage->engine->name.starts_with("System");
    }
}

bool RestorerFromBackup::TableKey::operator ==(const TableKey & right) const
{
    return (name == right.name) && (is_temporary == right.is_temporary);
}

bool RestorerFromBackup::TableKey::operator <(const TableKey & right) const
{
    return (name < right.name) || ((name == right.name) && (is_temporary < right.is_temporary));
}

std::string_view RestorerFromBackup::toString(Stage stage)
{
    switch (stage)
    {
        case Stage::kPreparing: return "Preparing";
        case Stage::kFindingTablesInBackup: return "Finding tables in backup";
        case Stage::kCreatingDatabases: return "Creating databases";
        case Stage::kCreatingTables: return "Creating tables";
        case Stage::kInsertingDataToTables: return "Inserting data to tables";
        case Stage::kError: return "Error";
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown backup stage: {}", static_cast<int>(stage));
}


RestorerFromBackup::RestorerFromBackup(
    const ASTBackupQuery::Elements & restore_query_elements_,
    const RestoreSettings & restore_settings_,
    std::shared_ptr<IRestoreCoordination> restore_coordination_,
    const BackupPtr & backup_,
    const ContextMutablePtr & context_,
    std::chrono::seconds timeout_)
    : restore_query_elements(restore_query_elements_)
    , restore_settings(restore_settings_)
    , restore_coordination(restore_coordination_)
    , backup(backup_)
    , context(context_)
    , timeout(timeout_)
    , log(&Poco::Logger::get("RestorerFromBackup"))
{
}

RestorerFromBackup::~RestorerFromBackup() = default;

void RestorerFromBackup::restoreMetadata()
{
    try
    {
        /// restoreMetadata() must not be called multiple times.
        if (current_stage != Stage::kPreparing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Already restoring");

        /// Calculate the root path in the backup for restoring, it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
        findRootPathsInBackup();

        /// Do renaming in the create queries according to the renaming config.
        renaming_map = makeRenamingMapFromBackupQuery(restore_query_elements);

        /// Find all the databases and tables which we will read from the backup.
        setStage(Stage::kFindingTablesInBackup);
        collectDatabaseAndTableInfos();

        /// Create databases using the create queries read from the backup.
        setStage(Stage::kCreatingDatabases);
        createDatabases();

        /// Create tables using the create queries read from the backup.
        setStage(Stage::kCreatingTables);
        createTables();

        /// All what's left is to insert data to tables.
        /// No more data restoring tasks are allowed after this point.
        setStage(Stage::kInsertingDataToTables);
    }
    catch (...)
    {
        try
        {
            /// Other hosts should know that we've encountered an error.
            setStage(Stage::kError, getCurrentExceptionMessage(false));
        }
        catch (...)
        {
        }
        throw;
    }
}

RestorerFromBackup::DataRestoreTasks RestorerFromBackup::getDataRestoreTasks()
{
    if (current_stage != Stage::kInsertingDataToTables)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata wasn't restored");

    if (data_restore_tasks.empty())
        return {};

    LOG_TRACE(log, "Will insert data to tables");

    /// Storages and table locks must exist while we're executing data restoring tasks.
    auto storages = std::make_shared<std::vector<StoragePtr>>();
    auto table_locks = std::make_shared<std::vector<TableLockHolder>>();
    storages->reserve(table_infos.size());
    table_locks->reserve(table_infos.size());
    for (const auto & table_info : table_infos | boost::adaptors::map_values)
    {
        storages->push_back(table_info.storage);
        table_locks->push_back(table_info.table_lock);
    }

    DataRestoreTasks res_tasks;
    for (const auto & task : data_restore_tasks)
        res_tasks.push_back([task, storages, table_locks] { task(); });

    return res_tasks;
}

void RestorerFromBackup::setStage(Stage new_stage, const String & error_message)
{
    if (new_stage == Stage::kError)
        LOG_ERROR(log, "{} failed with error: {}", toString(current_stage), error_message);
    else
        LOG_TRACE(log, "{}", toString(new_stage));
    
    current_stage = new_stage;
    
    if (new_stage == Stage::kError)
    {
        restore_coordination->syncStageError(restore_settings.host_id, error_message);
    }
    else
    {
        auto all_hosts
            = BackupSettings::Util::filterHostIDs(restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);
        restore_coordination->syncStage(restore_settings.host_id, static_cast<int>(new_stage), all_hosts, timeout);
    }
}

void RestorerFromBackup::findRootPathsInBackup()
{
    size_t shard_num = 1;
    size_t replica_num = 1;
    if (!restore_settings.host_id.empty())
    {
        std::tie(shard_num, replica_num)
            = BackupSettings::Util::findShardNumAndReplicaNum(restore_settings.cluster_host_ids, restore_settings.host_id);
    }
    
    root_paths_in_backup.clear();

    /// Start with "" as the root path and then we will add shard- and replica-related part to it.
    fs::path root_path = "/";
    root_paths_in_backup.push_back(root_path);

    /// Add shard-related part to the root path.
    Strings shards_in_backup = backup->listFiles(root_path / "shards");
    if (shards_in_backup.empty())
    {
        if (restore_settings.shard_num_in_backup > 1)
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", restore_settings.shard_num_in_backup);
    }
    else
    {
        String shard_name;
        if (restore_settings.shard_num_in_backup)
            shard_name = std::to_string(restore_settings.shard_num_in_backup);
        else if (shards_in_backup.size() == 1)
            shard_name = shards_in_backup.front();
        else
            shard_name = std::to_string(shard_num);
        if (std::find(shards_in_backup.begin(), shards_in_backup.end(), shard_name) == shards_in_backup.end())
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", shard_name);
        root_path = root_path / "shards" / shard_name;
        root_paths_in_backup.push_back(root_path);
    }

    /// Add replica-related part to the root path.
    Strings replicas_in_backup = backup->listFiles(root_path / "replicas");
    if (replicas_in_backup.empty())
    {
        if (restore_settings.replica_num_in_backup > 1)
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No replica #{} in backup", restore_settings.replica_num_in_backup);
    }
    else
    {
        String replica_name;
        if (restore_settings.replica_num_in_backup)
        {
            replica_name = std::to_string(restore_settings.replica_num_in_backup);
            if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), replica_name) == replicas_in_backup.end())
                throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No replica #{} in backup", replica_name);
        }
        else
        {
            replica_name = std::to_string(replica_num);
            if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), replica_name) == replicas_in_backup.end())
                replica_name = replicas_in_backup.front();
        }
        root_path = root_path / "replicas" / replica_name;
        root_paths_in_backup.push_back(root_path);
    }

    /// Revert the list of root paths, because we need it in the following order:
    /// "/shards/<shard_num>/replicas/<replica_num>/" (first we search tables here)
    /// "/shards/<shard_num>/" (then here)
    /// "/" (and finally here)
    std::reverse(root_paths_in_backup.begin(), root_paths_in_backup.end());

    LOG_TRACE(
        log,
        "Will use paths in backup: {}",
        boost::algorithm::join(
            root_paths_in_backup
                | boost::adaptors::transformed([](const fs::path & path) -> String { return doubleQuoteString(String{path}); }),
            ", "));
}

void RestorerFromBackup::collectDatabaseAndTableInfos()
{
    database_infos.clear();
    table_infos.clear();
    for (const auto & element : restore_query_elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::ElementType::TABLE:
            {
                collectTableInfo({element.database_name, element.table_name}, false, element.partitions);
                break;
            }
            case ASTBackupQuery::ElementType::TEMPORARY_TABLE:
            {
                collectTableInfo({element.database_name, element.table_name}, true, element.partitions);
                break;
            }
            case ASTBackupQuery::ElementType::DATABASE:
            {
                collectDatabaseInfo(element.database_name, element.except_tables, /* throw_if_no_database_metadata_in_backup= */ true);
                break;
            }
            case ASTBackupQuery::ElementType::ALL:
            {
                collectAllDatabasesInfo(element.except_databases, element.except_tables);
                break;
            }
        }
    }

    LOG_INFO(log, "Will restore {} databases and {} tables", database_infos.size(), table_infos.size());
}

void RestorerFromBackup::collectTableInfo(const QualifiedTableName & table_name_in_backup, bool is_temporary_table, const std::optional<ASTs> & partitions)
{
    String database_name_in_backup = is_temporary_table ? DatabaseCatalog::TEMPORARY_DATABASE : table_name_in_backup.database;

    std::optional<fs::path> metadata_path;
    std::optional<fs::path> root_path_in_use;
    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        fs::path try_metadata_path;
        if (is_temporary_table)
        {
            try_metadata_path
                = root_path_in_backup / "temporary_tables" / "metadata" / (escapeForFileName(table_name_in_backup.table) + ".sql");
        }
        else
        {
            try_metadata_path = root_path_in_backup / "metadata" / escapeForFileName(table_name_in_backup.database)
                / (escapeForFileName(table_name_in_backup.table) + ".sql");
        }

        if (backup->fileExists(try_metadata_path))
        {
            metadata_path = try_metadata_path;
            root_path_in_use = root_path_in_backup;
            break;
        }
    }

    if (!metadata_path)
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Table {} not found in backup", table_name_in_backup.getFullName());

    TableKey table_key;
    fs::path data_path_in_backup;
    if (is_temporary_table)
    {
        data_path_in_backup = *root_path_in_use / "temporary_tables" / "data" / escapeForFileName(table_name_in_backup.table);
        table_key.name.table = renaming_map.getNewTemporaryTableName(table_name_in_backup.table);
        table_key.is_temporary = true;
    }
    else
    {
        data_path_in_backup
            = *root_path_in_use / "data" / escapeForFileName(table_name_in_backup.database) / escapeForFileName(table_name_in_backup.table);
        table_key.name = renaming_map.getNewTableName(table_name_in_backup);
    }

    auto read_buffer = backup->readFile(*metadata_path)->getReadBuffer();
    String create_query_str;
    readStringUntilEOF(create_query_str, *read_buffer);
    read_buffer.reset();
    ParserCreateQuery create_parser;
    ASTPtr create_table_query = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    renameDatabaseAndTableNameInCreateQuery(context->getGlobalContext(), renaming_map, create_table_query);

    if (auto it = table_infos.find(table_key); it != table_infos.end())
    {
        const TableInfo & table_info = it->second;
        if (table_info.create_table_query && (serializeAST(*table_info.create_table_query) != serializeAST(*create_table_query)))
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Extracted two different create queries for the same {}table {}: {} and {}",
                (is_temporary_table ? "temporary " : ""),
                table_key.name.getFullName(),
                serializeAST(*table_info.create_table_query),
                serializeAST(*create_table_query));
        }
    }

    TableInfo & res_table_info = table_infos[table_key];
    res_table_info.create_table_query = create_table_query;
    res_table_info.data_path_in_backup = data_path_in_backup;
    res_table_info.dependencies = getDependenciesSetFromCreateQuery(context->getGlobalContext(), table_key.name, create_table_query);

    if (partitions)
    {
        if (!res_table_info.partitions)
            res_table_info.partitions.emplace();
        insertAtEnd(*res_table_info.partitions, *partitions);
    }
}

void RestorerFromBackup::collectDatabaseInfo(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names, bool throw_if_no_database_metadata_in_backup)
{
    std::optional<fs::path> metadata_path;
    std::unordered_set<String> table_names_in_backup;
    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        fs::path try_metadata_path = root_path_in_backup / "metadata" / (escapeForFileName(database_name_in_backup) + ".sql");
        if (!metadata_path && backup->fileExists(try_metadata_path))
            metadata_path = try_metadata_path;

        Strings file_names = backup->listFiles(root_path_in_backup / "metadata" / escapeForFileName(database_name_in_backup));
        for (const String & file_name : file_names)
        {
            if (!file_name.ends_with(sql_ext))
                continue;
            String file_name_without_ext = file_name.substr(0, file_name.length() - sql_ext.length());
            table_names_in_backup.insert(unescapeForFileName(file_name_without_ext));
        }
    }

    if (!metadata_path && throw_if_no_database_metadata_in_backup)
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Database {} not found in backup", backQuoteIfNeed(database_name_in_backup));

    if (metadata_path)
    {
        auto read_buffer = backup->readFile(*metadata_path)->getReadBuffer();
        String create_query_str;
        readStringUntilEOF(create_query_str, *read_buffer);
        read_buffer.reset();
        ParserCreateQuery create_parser;
        ASTPtr create_database_query = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        renameDatabaseAndTableNameInCreateQuery(context->getGlobalContext(), renaming_map, create_database_query);

        String database_name = renaming_map.getNewDatabaseName(database_name_in_backup);
        DatabaseInfo & database_info = database_infos[database_name];
        
        if (database_info.create_database_query && (serializeAST(*database_info.create_database_query) != serializeAST(*create_database_query)))
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_DATABASE,
                "Extracted two different create queries for the same database {}: {} and {}",
                backQuoteIfNeed(database_name),
                serializeAST(*database_info.create_database_query),
                serializeAST(*create_database_query));
        }

        database_info.create_database_query = create_database_query;
    }

    for (const String & table_name_in_backup : table_names_in_backup)
    {
        if (except_table_names.contains({database_name_in_backup, table_name_in_backup}))
            continue;

        collectTableInfo({database_name_in_backup, table_name_in_backup}, /* is_temporary_table= */ false, /* partitions= */ {});
    }
}

void RestorerFromBackup::collectAllDatabasesInfo(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names)
{
    std::unordered_set<String> database_names_in_backup;
    std::unordered_set<String> temporary_table_names_in_backup;

    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        Strings file_names = backup->listFiles(root_path_in_backup / "metadata");
        for (String & file_name : file_names)
        {
            if (file_name.ends_with(sql_ext))
                file_name.resize(file_name.length() - sql_ext.length());
            database_names_in_backup.emplace(unescapeForFileName(file_name));
        }

        file_names = backup->listFiles(root_path_in_backup / "temporary_tables" / "metadata");
        for (String & file_name : file_names)
        {
            if (!file_name.ends_with(sql_ext))
                continue;
            file_name.resize(file_name.length() - sql_ext.length());
            temporary_table_names_in_backup.emplace(unescapeForFileName(file_name));
        }
    }

    for (const String & database_name_in_backup : database_names_in_backup)
    {
        if (except_database_names.contains(database_name_in_backup))
            continue;

        collectDatabaseInfo(database_name_in_backup, except_table_names, /* throw_if_no_database_metadata_in_backup= */ false);
    }

    for (const String & temporary_table_name_in_backup : temporary_table_names_in_backup)
        collectTableInfo({"", temporary_table_name_in_backup}, /* is_temporary_table= */ true, /* partitions= */ {});
}

void RestorerFromBackup::createDatabases()
{
    for (const auto & [database_name, database_info] : database_infos)
    {
        bool need_create_database = (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist);
        if (need_create_database && DatabaseCatalog::isPredefinedDatabaseName(database_name))
            need_create_database = false; /// Predefined databases always exist.

        if (need_create_database)
        {
            /// Execute CREATE DATABASE query.
            auto create_database_query = database_info.create_database_query;
            if (restore_settings.create_table == RestoreTableCreationMode::kCreateIfNotExists)
            {
                create_database_query = create_database_query->clone();
                create_database_query->as<ASTCreateQuery &>().if_not_exists = true;
            }
            LOG_TRACE(log, "Creating database {}: {}", backQuoteIfNeed(database_name), serializeAST(*create_database_query));
            executeCreateQuery(create_database_query);
        }

        DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

        if (!restore_settings.allow_different_database_def)
        {
            /// Check that the database's definition is the same as expected.
            ASTPtr create_database_query = database->getCreateDatabaseQueryForBackup();
            ASTPtr expected_create_query = database_info.create_database_query;
            if (serializeAST(*create_database_query) != serializeAST(*expected_create_query))
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_DATABASE,
                    "The database {} has a different definition: {} "
                    "comparing to its definition in the backup: {}",
                    backQuoteIfNeed(database_name),
                    serializeAST(*create_database_query),
                    serializeAST(*expected_create_query));
            }
        }
    }
}

void RestorerFromBackup::createTables()
{
    while (true)
    {
        /// We need to create tables considering their dependencies.
        auto tables_to_create = findTablesWithoutDependencies();
        if (tables_to_create.empty())
            break; /// We've already created all the tables.

        for (const auto & table_key : tables_to_create)
        {
            auto & table_info = table_infos.at(table_key);

            DatabasePtr database;
            if (table_key.is_temporary)
                database = DatabaseCatalog::instance().getDatabaseForTemporaryTables();
            else
                database = DatabaseCatalog::instance().getDatabase(table_key.name.database);

            bool need_create_table = (restore_settings.create_table != RestoreTableCreationMode::kMustExist);
            if (need_create_table && hasSystemTableEngine(*table_info.create_table_query))
                need_create_table = false; /// Tables with System* table engine already exist or can't be created by SQL anyway.

            if (need_create_table)
            {
                /// Execute CREATE TABLE query (we call IDatabase::createTableRestoredFromBackup() to allow the database to do some
                /// database-specific things).
                auto create_table_query = table_info.create_table_query;
                if (restore_settings.create_table == RestoreTableCreationMode::kCreateIfNotExists)
                {
                    create_table_query = create_table_query->clone();
                    create_table_query->as<ASTCreateQuery &>().if_not_exists = true;
                }
                LOG_TRACE(
                    log,
                    "Creating {}table {}: {}",
                    (table_key.is_temporary ? "temporary " : ""),
                    table_key.name.getFullName(),
                    serializeAST(*create_table_query));

                database->createTableRestoredFromBackup(create_table_query, *this);
            }

            table_info.created = true;

            auto resolved_id = table_key.is_temporary
                ? context->resolveStorageID(StorageID{"", table_key.name.table}, Context::ResolveExternal)
                : context->resolveStorageID(StorageID{table_key.name.database, table_key.name.table}, Context::ResolveGlobal);

            auto storage = database->getTable(resolved_id.table_name, context);
            table_info.storage = storage;
            table_info.table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);

            if (!restore_settings.allow_different_table_def)
            {
                ASTPtr create_table_query = storage->getCreateQueryForBackup(context);
                ASTPtr expected_create_query = table_info.create_table_query;
                if (serializeAST(*create_table_query) != serializeAST(*expected_create_query))
                {
                    throw Exception(
                        ErrorCodes::CANNOT_RESTORE_TABLE,
                        "The {}table {} has a different definition: {} "
                        "comparing to its definition in the backup: {}",
                        (table_key.is_temporary ? "temporary " : ""),
                        table_key.name.getFullName(),
                        serializeAST(*create_table_query),
                        serializeAST(*expected_create_query));
                }
            }

            if (!restore_settings.structure_only)
            {
                const auto & data_path_in_backup = table_info.data_path_in_backup;
                const auto & partitions = table_info.partitions;
                storage->restoreDataFromBackup(*this, data_path_in_backup, partitions);
            }
        }
    }
}

/// Returns the list of tables without dependencies or those which dependencies have been created before.
std::vector<RestorerFromBackup::TableKey> RestorerFromBackup::findTablesWithoutDependencies() const
{
    std::vector<TableKey> tables_without_dependencies;
    bool all_tables_created = true;

    for (const auto & [key, table_info] : table_infos)
    {
        if (table_info.created)
            continue;

        /// Found a table which is not created yet.
        all_tables_created = false;

        /// Check if all dependencies have been created before.
        bool all_dependencies_met = true;
        for (const auto & dependency : table_info.dependencies)
        {
            auto it = table_infos.find(TableKey{dependency, false});
            if ((it != table_infos.end()) && !it->second.created)
            {
                all_dependencies_met = false;
                break;
            }
        }

        if (all_dependencies_met)
            tables_without_dependencies.push_back(key);
    }

    if (!tables_without_dependencies.empty())
        return tables_without_dependencies;

    if (all_tables_created)
        return {};

    /// Cyclic dependency? We'll try to create those tables anyway but probably it's going to fail.
    std::vector<TableKey> tables_with_cyclic_dependencies;
    for (const auto & [key, table_info] : table_infos)
    {
        if (!table_info.created)
            tables_with_cyclic_dependencies.push_back(key);
    }

    /// Only show a warning here, proper exception will be thrown later on creating those tables.
    LOG_WARNING(
        log,
        "Some tables have cyclic dependency from each other: {}",
        boost::algorithm::join(
            tables_with_cyclic_dependencies
                | boost::adaptors::transformed([](const TableKey & key) -> String { return key.name.getFullName(); }),
            ", "));

    return tables_with_cyclic_dependencies;
}

void RestorerFromBackup::addDataRestoreTask(DataRestoreTask && new_task)
{
    if (current_stage == Stage::kInsertingDataToTables)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding data-restoring tasks is not allowed");
    data_restore_tasks.push_back(std::move(new_task));
}

void RestorerFromBackup::addDataRestoreTasks(DataRestoreTasks && new_tasks)
{
    if (current_stage == Stage::kInsertingDataToTables)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding data-restoring tasks is not allowed");
    insertAtEnd(data_restore_tasks, std::move(new_tasks));
}

void RestorerFromBackup::addAccessRestorePathInBackup(const String & data_path)
{
    bool first_access_restore_path = !access_restore_task;
    if (first_access_restore_path)
        access_restore_task = std::make_shared<AccessRestoreTask>(backup, restore_settings, restore_coordination);
 
    access_restore_task->addDataPath(data_path);

    if (first_access_restore_path)
        addDataRestoreTask([task = access_restore_task, access_control = &context->getAccessControl()] { task->restore(*access_control); });
} 

void RestorerFromBackup::executeCreateQuery(const ASTPtr & create_query) const
{
    InterpreterCreateQuery interpreter{create_query, context};
    interpreter.setInternal(true);
    interpreter.execute();
}

void RestorerFromBackup::throwPartitionsNotSupported(const StorageID & storage_id, const String & table_engine)
{
    throw Exception(
        ErrorCodes::CANNOT_RESTORE_TABLE,
        "Table engine {} doesn't support partitions, cannot  table {}",
        table_engine,
        storage_id.getFullTableName());
}

void RestorerFromBackup::throwTableIsNotEmpty(const StorageID & storage_id)
{
    throw Exception(
        ErrorCodes::CANNOT_RESTORE_TABLE,
        "Cannot restore the table {} because it already contains some data. You can set structure_only=true or "
        "allow_non_empty_tables=true to overcome that in the way you want",
        storage_id.getFullTableName());
}
}
