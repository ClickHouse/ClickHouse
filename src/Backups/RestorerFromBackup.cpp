#include <Backups/RestorerFromBackup.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupSettings.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Access/AccessBackup.h>
#include <Access/AccessRights.h>
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
    extern const int LOGICAL_ERROR;
}


namespace
{
    /// Finding databases and tables in the backup which we're going to restore.
    constexpr const char * kFindingTablesInBackupStatus = "finding tables in backup";

    /// Creating databases or finding them and checking their definitions.
    constexpr const char * kCreatingDatabasesStatus = "creating databases";

    /// Creating tables or finding them and checking their definition.
    constexpr const char * kCreatingTablesStatus = "creating tables";

    /// Inserting restored data to tables.
    constexpr const char * kInsertingDataToTablesStatus = "inserting data to tables";

    /// Uppercases the first character of a passed string.
    String toUpperFirst(const String & str)
    {
        String res = str;
        res[0] = std::toupper(res[0]);
        return res;
    }

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

    /// Whether a specified name corresponds one of the tables backuping ACL.
    bool isSystemAccessTableName(const QualifiedTableName & table_name)
    {
        if (table_name.database != DatabaseCatalog::SYSTEM_DATABASE)
            return false;

        return (table_name.table == "users") || (table_name.table == "roles") || (table_name.table == "settings_profiles")
            || (table_name.table == "row_policies") || (table_name.table == "quotas");
    }

    /// Whether a specified name corresponds one of the tables backuping ACL.
    bool isSystemFunctionsTableName(const QualifiedTableName & table_name)
    {
        return (table_name.database == DatabaseCatalog::SYSTEM_DATABASE) && (table_name.table == "functions");
    }
 }


RestorerFromBackup::RestorerFromBackup(
    const ASTBackupQuery::Elements & restore_query_elements_,
    const RestoreSettings & restore_settings_,
    std::shared_ptr<IRestoreCoordination> restore_coordination_,
    const BackupPtr & backup_,
    const ContextMutablePtr & context_)
    : restore_query_elements(restore_query_elements_)
    , restore_settings(restore_settings_)
    , restore_coordination(restore_coordination_)
    , backup(backup_)
    , context(context_)
    , create_table_timeout(context->getConfigRef().getUInt64("backups.create_table_timeout", 300000))
    , log(&Poco::Logger::get("RestorerFromBackup"))
{
}

RestorerFromBackup::~RestorerFromBackup() = default;

RestorerFromBackup::DataRestoreTasks RestorerFromBackup::run(Mode mode)
{
    /// run() can be called onle once.
    if (!current_status.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Already restoring");

    /// Find other hosts working along with us to execute this ON CLUSTER query.
    all_hosts = BackupSettings::Util::filterHostIDs(
        restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num);

    /// Do renaming in the create queries according to the renaming config.
    renaming_map = makeRenamingMapFromBackupQuery(restore_query_elements);

    /// Calculate the root path in the backup for restoring, it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
    findRootPathsInBackup();

    /// Find all the databases and tables which we will read from the backup.
    setStatus(kFindingTablesInBackupStatus);
    findDatabasesAndTablesInBackup();

    /// Check access rights.
    checkAccessForObjectsFoundInBackup();

    if (mode == Mode::CHECK_ACCESS_ONLY)
        return {};

    /// Create databases using the create queries read from the backup.
    setStatus(kCreatingDatabasesStatus);
    createDatabases();

    /// Create tables using the create queries read from the backup.
    setStatus(kCreatingTablesStatus);
    createTables();

    /// All what's left is to insert data to tables.
    /// No more data restoring tasks are allowed after this point.
    setStatus(kInsertingDataToTablesStatus);
    return getDataRestoreTasks();
}

void RestorerFromBackup::setStatus(const String & new_status, const String & message)
{
    LOG_TRACE(log, "{}", toUpperFirst(new_status));
    current_status = new_status;
    if (restore_coordination)
    {
        restore_coordination->setStatus(restore_settings.host_id, new_status, message);
        restore_coordination->waitStatus(all_hosts, new_status);
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

void RestorerFromBackup::findDatabasesAndTablesInBackup()
{
    database_infos.clear();
    table_infos.clear();
    for (const auto & element : restore_query_elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::ElementType::TABLE:
            {
                findTableInBackup({element.database_name, element.table_name}, element.partitions);
                break;
            }
            case ASTBackupQuery::ElementType::TEMPORARY_TABLE:
            {
                findTableInBackup({DatabaseCatalog::TEMPORARY_DATABASE, element.table_name}, element.partitions);
                break;
            }
            case ASTBackupQuery::ElementType::DATABASE:
            {
                findDatabaseInBackup(element.database_name, element.except_tables);
                break;
            }
            case ASTBackupQuery::ElementType::ALL:
            {
                findEverythingInBackup(element.except_databases, element.except_tables);
                break;
            }
        }
    }

    LOG_INFO(log, "Will restore {} databases and {} tables", database_infos.size(), table_infos.size());
}

void RestorerFromBackup::findTableInBackup(const QualifiedTableName & table_name_in_backup, const std::optional<ASTs> & partitions)
{
    bool is_temporary_table = (table_name_in_backup.database == DatabaseCatalog::TEMPORARY_DATABASE);

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
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "{} not found in backup",
            tableNameWithTypeToString(table_name_in_backup.database, table_name_in_backup.table, true));

    fs::path data_path_in_backup;
    if (is_temporary_table)
    {
        data_path_in_backup = *root_path_in_use / "temporary_tables" / "data" / escapeForFileName(table_name_in_backup.table);
    }
    else
    {
        data_path_in_backup
            = *root_path_in_use / "data" / escapeForFileName(table_name_in_backup.database) / escapeForFileName(table_name_in_backup.table);
    }

    auto read_buffer = backup->readFile(*metadata_path)->getReadBuffer();
    String create_query_str;
    readStringUntilEOF(create_query_str, *read_buffer);
    read_buffer.reset();
    ParserCreateQuery create_parser;
    ASTPtr create_table_query = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    renameDatabaseAndTableNameInCreateQuery(create_table_query, renaming_map, context->getGlobalContext());

    QualifiedTableName table_name = renaming_map.getNewTableName(table_name_in_backup);

    if (auto it = table_infos.find(table_name); it != table_infos.end())
    {
        const TableInfo & table_info = it->second;
        if (table_info.create_table_query && (serializeAST(*table_info.create_table_query) != serializeAST(*create_table_query)))
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Extracted two different create queries for the same {}: {} and {}",
                tableNameWithTypeToString(table_name.database, table_name.table, false),
                serializeAST(*table_info.create_table_query),
                serializeAST(*create_table_query));
        }
    }

    TableInfo & res_table_info = table_infos[table_name];
    res_table_info.create_table_query = create_table_query;
    res_table_info.is_predefined_table = DatabaseCatalog::instance().isPredefinedTable(StorageID{table_name.database, table_name.table});
    res_table_info.dependencies = getDependenciesSetFromCreateQuery(context->getGlobalContext(), table_name, create_table_query);
    res_table_info.has_data = backup->hasFiles(data_path_in_backup);
    res_table_info.data_path_in_backup = data_path_in_backup;

    if (partitions)
    {
        if (!res_table_info.partitions)
            res_table_info.partitions.emplace();
        insertAtEnd(*res_table_info.partitions, *partitions);
    }

    /// Special handling for ACL-related system tables.
    if (!restore_settings.structure_only && isSystemAccessTableName(table_name))
    {
        if (!access_restorer)
            access_restorer = std::make_unique<AccessRestorerFromBackup>(backup, restore_settings);

        try
        {
            /// addDataPath() will parse access*.txt files and extract access entities from them.
            /// We need to do that early because we need those access entities to check access.
            access_restorer->addDataPath(data_path_in_backup);
        }
        catch (Exception & e)
        {
            e.addMessage("While parsing data of {} from backup", tableNameWithTypeToString(table_name.database, table_name.table, false));
            throw;
        }
    }
}

void RestorerFromBackup::findDatabaseInBackup(const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names)
{
    std::optional<fs::path> metadata_path;
    std::unordered_set<String> table_names_in_backup;
    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        fs::path try_metadata_path, try_tables_metadata_path;
        if (database_name_in_backup == DatabaseCatalog::TEMPORARY_DATABASE)
        {
            try_tables_metadata_path = root_path_in_backup / "temporary_tables" / "metadata";
        }
        else
        {
            try_metadata_path = root_path_in_backup / "metadata" / (escapeForFileName(database_name_in_backup) + ".sql");
            try_tables_metadata_path = root_path_in_backup / "metadata" / escapeForFileName(database_name_in_backup);
        }

        if (!metadata_path && !try_metadata_path.empty() && backup->fileExists(try_metadata_path))
            metadata_path = try_metadata_path;

        Strings file_names = backup->listFiles(try_tables_metadata_path);
        for (const String & file_name : file_names)
        {
            if (!file_name.ends_with(".sql"))
                continue;
            String file_name_without_ext = file_name.substr(0, file_name.length() - strlen(".sql"));
            table_names_in_backup.insert(unescapeForFileName(file_name_without_ext));
        }
    }

    if (!metadata_path && table_names_in_backup.empty())
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Database {} not found in backup", backQuoteIfNeed(database_name_in_backup));

    if (metadata_path)
    {
        auto read_buffer = backup->readFile(*metadata_path)->getReadBuffer();
        String create_query_str;
        readStringUntilEOF(create_query_str, *read_buffer);
        read_buffer.reset();
        ParserCreateQuery create_parser;
        ASTPtr create_database_query = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        renameDatabaseAndTableNameInCreateQuery(create_database_query, renaming_map, context->getGlobalContext());

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
        database_info.is_predefined_database = DatabaseCatalog::isPredefinedDatabase(database_name);
    }

    for (const String & table_name_in_backup : table_names_in_backup)
    {
        if (except_table_names.contains({database_name_in_backup, table_name_in_backup}))
            continue;

        findTableInBackup({database_name_in_backup, table_name_in_backup}, /* partitions= */ {});
    }
}

void RestorerFromBackup::findEverythingInBackup(const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names)
{
    std::unordered_set<String> database_names_in_backup;

    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        Strings file_names = backup->listFiles(root_path_in_backup / "metadata");
        for (String & file_name : file_names)
        {
            if (file_name.ends_with(".sql"))
                file_name.resize(file_name.length() - strlen(".sql"));
            database_names_in_backup.emplace(unescapeForFileName(file_name));
        }

        if (backup->hasFiles(root_path_in_backup / "temporary_tables" / "metadata"))
            database_names_in_backup.emplace(DatabaseCatalog::TEMPORARY_DATABASE);
    }

    for (const String & database_name_in_backup : database_names_in_backup)
    {
        if (except_database_names.contains(database_name_in_backup))
            continue;

        findDatabaseInBackup(database_name_in_backup, except_table_names);
    }
}

void RestorerFromBackup::checkAccessForObjectsFoundInBackup() const
{
    AccessRightsElements required_access;
    for (const auto & [database_name, database_info] : database_infos)
    {
        if (database_info.is_predefined_database)
            continue;

        AccessFlags flags;

        if (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist)
            flags |= AccessType::CREATE_DATABASE;

        if (!flags)
            flags = AccessType::SHOW_DATABASES;

        required_access.emplace_back(flags, database_name);
    }

    for (const auto & [table_name, table_info] : table_infos)
    {
        if (table_info.is_predefined_table)
        {
            if (isSystemFunctionsTableName(table_name))
            {
                /// CREATE_FUNCTION privilege is required to restore the "system.functions" table.
                if (!restore_settings.structure_only && table_info.has_data)
                    required_access.emplace_back(AccessType::CREATE_FUNCTION);
            }
            /// Privileges required to restore ACL system tables are checked separately
            /// (see access_restore_task->getRequiredAccess() below).
            continue;
        }

        if (table_name.database == DatabaseCatalog::TEMPORARY_DATABASE)
        {
            if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
            continue;
        }

        AccessFlags flags;
        const ASTCreateQuery & create = table_info.create_table_query->as<const ASTCreateQuery &>();

        if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
        {
            if (create.is_dictionary)
                flags |= AccessType::CREATE_DICTIONARY;
            else if (create.is_ordinary_view || create.is_materialized_view || create.is_live_view)
                flags |= AccessType::CREATE_VIEW;
            else
                flags |= AccessType::CREATE_TABLE;
        }

        if (!restore_settings.structure_only && table_info.has_data)
        {
            flags |= AccessType::INSERT;
        }

        if (!flags)
        {
            if (create.is_dictionary)
                flags = AccessType::SHOW_DICTIONARIES;
            else
                flags = AccessType::SHOW_TABLES;
        }

        required_access.emplace_back(flags, table_name.database, table_name.table);
    }

    if (access_restorer)
        insertAtEnd(required_access, access_restorer->getRequiredAccess());

    /// We convert to AccessRights and back to check access rights in a predictable way
    /// (some elements could be duplicated or not sorted).
    required_access = AccessRights{required_access}.getElements();

    context->checkAccess(required_access);
}

void RestorerFromBackup::createDatabases()
{
    for (const auto & database_name : database_infos | boost::adaptors::map_keys)
    {
        createDatabase(database_name);
        checkDatabase(database_name);
    }
}

void RestorerFromBackup::createDatabase(const String & database_name) const
{
    if (restore_settings.create_database == RestoreDatabaseCreationMode::kMustExist)
        return;

    /// Predefined databases always exist.
    const auto & database_info = database_infos.at(database_name);
    if (database_info.is_predefined_database)
        return;

    auto create_database_query = database_info.create_database_query;
    if (restore_settings.create_table == RestoreTableCreationMode::kCreateIfNotExists)
    {
        create_database_query = create_database_query->clone();
        create_database_query->as<ASTCreateQuery &>().if_not_exists = true;
    }

    LOG_TRACE(log, "Creating database {}: {}", backQuoteIfNeed(database_name), serializeAST(*create_database_query));

    try
    {
        /// Execute CREATE DATABASE query.
        InterpreterCreateQuery interpreter{create_database_query, context};
        interpreter.setInternal(true);
        interpreter.execute();
    }
    catch (Exception & e)
    {
        e.addMessage("While creating database {}", backQuoteIfNeed(database_name));
        throw;
    }
}

void RestorerFromBackup::checkDatabase(const String & database_name)
{
    auto & database_info = database_infos.at(database_name);
    try
    {
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);
        database_info.database = database;

        if (!restore_settings.allow_different_database_def && !database_info.is_predefined_database)
        {
            /// Check that the database's definition is the same as expected.
            ASTPtr create_database_query = database->getCreateDatabaseQuery();
            adjustCreateQueryForBackup(create_database_query, context->getGlobalContext(), nullptr);
            ASTPtr expected_create_query = database_info.create_database_query;
            if (serializeAST(*create_database_query) != serializeAST(*expected_create_query))
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_DATABASE,
                    "The database has a different definition: {} "
                    "comparing to its definition in the backup: {}",
                    serializeAST(*create_database_query),
                    serializeAST(*expected_create_query));
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage("While checking database {}", backQuoteIfNeed(database_name));
        throw;
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

        for (const auto & table_name : tables_to_create)
        {
            createTable(table_name);
            checkTable(table_name);
            insertDataToTable(table_name);
        }
    }
}

void RestorerFromBackup::createTable(const QualifiedTableName & table_name)
{
    if (restore_settings.create_table == RestoreTableCreationMode::kMustExist)
        return;

    /// Predefined tables always exist.
    auto & table_info = table_infos.at(table_name);
    if (table_info.is_predefined_table)
        return;

    auto create_table_query = table_info.create_table_query;
    if (restore_settings.create_table == RestoreTableCreationMode::kCreateIfNotExists)
    {
        create_table_query = create_table_query->clone();
        create_table_query->as<ASTCreateQuery &>().if_not_exists = true;
    }

    LOG_TRACE(
        log, "Creating {}: {}", tableNameWithTypeToString(table_name.database, table_name.table, false), serializeAST(*create_table_query));

    try
    {
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_name.database);
        table_info.database = database;

        /// Execute CREATE TABLE query (we call IDatabase::createTableRestoredFromBackup() to allow the database to do some
        /// database-specific things).
        database->createTableRestoredFromBackup(
            create_table_query,
            context,
            restore_coordination,
            std::chrono::duration_cast<std::chrono::milliseconds>(create_table_timeout).count());
    }
    catch (Exception & e)
    {
        e.addMessage("While creating {}", tableNameWithTypeToString(table_name.database, table_name.table, false));
        throw;
    }
}

void RestorerFromBackup::checkTable(const QualifiedTableName & table_name)
{
    auto & table_info = table_infos.at(table_name);
    auto database = table_info.database;

    try
    {
        if (!database)
        {
            database = DatabaseCatalog::instance().getDatabase(table_name.database);
            table_info.database = database;
        }

        auto resolved_id = (table_name.database == DatabaseCatalog::TEMPORARY_DATABASE)
            ? context->resolveStorageID(StorageID{"", table_name.table}, Context::ResolveExternal)
            : context->resolveStorageID(StorageID{table_name.database, table_name.table}, Context::ResolveGlobal);

        StoragePtr storage = database->getTable(resolved_id.table_name, context);
        table_info.storage = storage;
        table_info.table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);

        if (!restore_settings.allow_different_table_def && !table_info.is_predefined_table)
        {
            ASTPtr create_table_query = database->getCreateTableQuery(resolved_id.table_name, context);
            adjustCreateQueryForBackup(create_table_query, context->getGlobalContext(), nullptr);
            ASTPtr expected_create_query = table_info.create_table_query;
            if (serializeAST(*create_table_query) != serializeAST(*expected_create_query))
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "The table has a different definition: {} "
                    "comparing to its definition in the backup: {}",
                    serializeAST(*create_table_query),
                    serializeAST(*expected_create_query));
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage("While checking {}", tableNameWithTypeToString(table_name.database, table_name.table, false));
        throw;
    }
}

void RestorerFromBackup::insertDataToTable(const QualifiedTableName & table_name)
{
    if (restore_settings.structure_only)
        return;

    auto & table_info = table_infos.at(table_name);
    auto storage = table_info.storage;

    try
    {
        const auto & data_path_in_backup = table_info.data_path_in_backup;
        const auto & partitions = table_info.partitions;
        if (partitions && !storage->supportsBackupPartition())
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Table engine {} doesn't support partitions",
                storage->getName());
        }
        storage->restoreDataFromBackup(*this, data_path_in_backup, partitions);
    }
    catch (Exception & e)
    {
        e.addMessage("While restoring data of {}", tableNameWithTypeToString(table_name.database, table_name.table, false));
        throw;
    }
}

/// Returns the list of tables without dependencies or those which dependencies have been created before.
std::vector<QualifiedTableName> RestorerFromBackup::findTablesWithoutDependencies() const
{
    std::vector<QualifiedTableName> tables_without_dependencies;
    bool all_tables_created = true;

    for (const auto & [key, table_info] : table_infos)
    {
        if (table_info.storage)
            continue;

        /// Found a table which is not created yet.
        all_tables_created = false;

        /// Check if all dependencies have been created before.
        bool all_dependencies_met = true;
        for (const auto & dependency : table_info.dependencies)
        {
            auto it = table_infos.find(dependency);
            if ((it != table_infos.end()) && !it->second.storage)
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
    std::vector<QualifiedTableName> tables_with_cyclic_dependencies;
    for (const auto & [key, table_info] : table_infos)
    {
        if (!table_info.storage)
            tables_with_cyclic_dependencies.push_back(key);
    }

    /// Only show a warning here, proper exception will be thrown later on creating those tables.
    LOG_WARNING(
        log,
        "Some tables have cyclic dependency from each other: {}",
        boost::algorithm::join(
            tables_with_cyclic_dependencies
                | boost::adaptors::transformed([](const QualifiedTableName & table_name) -> String { return table_name.getFullName(); }),
            ", "));

    return tables_with_cyclic_dependencies;
}

void RestorerFromBackup::addDataRestoreTask(DataRestoreTask && new_task)
{
    if (current_status == kInsertingDataToTablesStatus)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding of data-restoring tasks is not allowed");
    data_restore_tasks.push_back(std::move(new_task));
}

void RestorerFromBackup::addDataRestoreTasks(DataRestoreTasks && new_tasks)
{
    if (current_status == kInsertingDataToTablesStatus)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding of data-restoring tasks is not allowed");
    insertAtEnd(data_restore_tasks, std::move(new_tasks));
}

RestorerFromBackup::DataRestoreTasks RestorerFromBackup::getDataRestoreTasks()
{
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

std::vector<std::pair<UUID, AccessEntityPtr>> RestorerFromBackup::getAccessEntitiesToRestore()
{
    if (!access_restorer || access_restored)
        return {};

    /// getAccessEntitiesToRestore() will return entities only when called first time (we don't want to restore the same entities again).
    access_restored = true;

    return access_restorer->getAccessEntities(context->getAccessControl());
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
