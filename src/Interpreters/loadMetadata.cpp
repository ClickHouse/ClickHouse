#include <Common/thread_local_rng.h>
#include <Common/ThreadPool.h>
#include <Common/PoolId.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/TablesLoader.h>
#include <Storages/StorageMaterializedView.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Core/Settings.h>

#include <filesystem>

#define ORDINARY_TO_ATOMIC_PREFIX ".tmp_convert."

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_database_ordinary;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType DistributedSend;
}

static void executeCreateQuery(
    const String & query,
    ContextMutablePtr context,
    const String & database,
    const String & file_name,
    bool create,
    bool has_force_restore_data_flag)
{
    const Settings & settings = context->getSettingsRef();
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "in file " + file_name,
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.setDatabase(database);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    if (!create)
    {
        interpreter.setForceAttach(true);
        interpreter.setForceRestoreData(has_force_restore_data_flag);
    }
    interpreter.setLoadDatabaseWithoutTables(true);
    interpreter.execute();
}

static bool isSystemOrInformationSchema(const String & database_name)
{
    return database_name == DatabaseCatalog::SYSTEM_DATABASE ||
           database_name == DatabaseCatalog::INFORMATION_SCHEMA ||
           database_name == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE;
}

static void loadDatabase(
    ContextMutablePtr context,
    const String & database,
    const String & database_path,
    bool force_restore_data)
{
    String database_attach_query;
    String database_metadata_file = database_path + ".sql";

    if (fs::exists(fs::path(database_metadata_file)))
    {
        /// There is .sql file with database creation statement.
        ReadBufferFromFile in(database_metadata_file, 1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else
    {
        /// It's first server run and we need create default and system databases.
        /// .sql file with database engine will be written for CREATE query.
        database_attach_query = "CREATE DATABASE " + backQuoteIfNeed(database);
    }

    try
    {
        executeCreateQuery(database_attach_query, context, database, database_metadata_file, /* create */ true, force_restore_data);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading database {} from path {}", backQuote(database), database_path));
        throw;
    }
}

static void checkUnsupportedVersion(ContextMutablePtr context, const String & database_name)
{
    /// Produce better exception message
    String metadata_path = context->getPath() + "metadata/" + database_name;
    if (fs::exists(fs::path(metadata_path)))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Data directory for {} database exists, but metadata file does not. "
                                                     "Probably you are trying to upgrade from version older than 20.7. "
                                                     "If so, you should upgrade through intermediate version.", database_name);
}

static void checkIncompleteOrdinaryToAtomicConversion(ContextPtr context, const std::map<String, String> & databases)
{
    if (context->getConfigRef().has("allow_reserved_database_name_tmp_convert"))
        return;

    auto convert_flag_path = fs::path(context->getFlagsPath()) / "convert_ordinary_to_atomic";
    if (!fs::exists(convert_flag_path))
        return;

    /// Flag exists. Let's check if we had an unsuccessful conversion attempt previously
    for (const auto & db : databases)
    {
        if (!db.first.starts_with(ORDINARY_TO_ATOMIC_PREFIX))
            continue;
        size_t last_dot = db.first.rfind('.');
        if (last_dot <= strlen(ORDINARY_TO_ATOMIC_PREFIX))
            continue;

        String actual_name = db.first.substr(strlen(ORDINARY_TO_ATOMIC_PREFIX), last_dot - strlen(ORDINARY_TO_ATOMIC_PREFIX));

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Found a database with special name: {}. "
                        "Most likely it indicates that conversion of database {} from Ordinary to Atomic "
                        "was interrupted or failed in the middle. You can add <allow_reserved_database_name_tmp_convert> to config.xml "
                        "or remove convert_ordinary_to_atomic file from flags/ directory, so the server will start forcefully. "
                        "After starting the server, you can finish conversion manually by moving rest of the tables from {} to {} "
                        "(using RENAME TABLE) and executing DROP DATABASE {} and RENAME DATABASE {} TO {}",
                        backQuote(db.first), backQuote(actual_name), backQuote(actual_name), backQuote(db.first),
                        backQuote(actual_name), backQuote(db.first), backQuote(actual_name));
    }
}

LoadTaskPtrs loadMetadata(ContextMutablePtr context, const String & default_database_name, bool async_load_databases)
{
    LoggerPtr log = getLogger("loadMetadata");

    String path = context->getPath() + "metadata";

    /// There may exist 'force_restore_data' file, which means skip safety threshold
    /// on difference of data parts while initializing tables.
    /// This file is immediately deleted i.e. "one-shot".
    auto force_restore_data_flag_file = fs::path(context->getFlagsPath()) / "force_restore_data";
    bool has_force_restore_data_flag = fs::exists(force_restore_data_flag_file);
    if (has_force_restore_data_flag)
    {
        try
        {
            fs::remove(force_restore_data_flag_file);
        }
        catch (...)
        {
            tryLogCurrentException("Load metadata", "Can't remove force restore file to enable data sanity checks");
        }
    }


    /// Loop over databases.
    std::map<String, String> databases;
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_symlink())
            continue;

        if (it->is_directory())
            continue;

        const auto current_file = it->path().filename().string();

        /// TODO: DETACH DATABASE PERMANENTLY ?
        if (fs::path(current_file).extension() == ".sql")
        {
            String db_name = fs::path(current_file).stem();
            if (!isSystemOrInformationSchema(db_name))
                databases.emplace(unescapeForFileName(db_name), fs::path(path) / db_name);
        }

        /// Temporary fails may be left from previous server runs.
        if (fs::path(current_file).extension() == ".tmp")
        {
            LOG_WARNING(log, "Removing temporary file {}", it->path().string());
            try
            {
                fs::remove(it->path());
            }
            catch (...)
            {
                /// It does not prevent server to startup.
                tryLogCurrentException(log);
            }
        }
    }

    checkIncompleteOrdinaryToAtomicConversion(context, databases);

    /// clickhouse-local creates DatabaseMemory as default database by itself
    /// For clickhouse-server we need create default database
    bool create_default_db_if_not_exists = !default_database_name.empty();
    bool metadata_dir_for_default_db_already_exists = databases.contains(default_database_name);
    if (create_default_db_if_not_exists && !metadata_dir_for_default_db_already_exists)
    {
        checkUnsupportedVersion(context, default_database_name);
        databases.emplace(default_database_name, std::filesystem::path(path) / escapeForFileName(default_database_name));
    }

    TablesLoader::Databases loaded_databases;
    for (const auto & [name, db_path] : databases)
    {
        loadDatabase(context, name, db_path, has_force_restore_data_flag);
        loaded_databases.insert({name, DatabaseCatalog::instance().getDatabase(name)});
    }

    auto mode = getLoadingStrictnessLevel(/* attach */ true, /* force_attach */ true, has_force_restore_data_flag, /*secondary*/ false);
    TablesLoader loader{context, std::move(loaded_databases), mode};
    auto load_tasks = loader.loadTablesAsync();
    auto startup_tasks = loader.startupTablesAsync();

    if (async_load_databases)
    {
        LOG_INFO(log, "Start asynchronous loading of databases");

        // Schedule all the jobs.
        // Note that to achieve behaviour similar to synchronous case (postponing of merges) we use priorities.
        // All startup jobs are assigned to pool with lower priority than load jobs pool.
        // So all tables will finish loading before the first table startup if there are no queries (or dependencies).
        // Query waiting for a table boosts its priority by moving jobs into `TablesLoaderForegroundPoolId` pool
        // to finish table startup faster than load of the other tables.
        scheduleLoad(load_tasks);
        scheduleLoad(startup_tasks);

        // Do NOT wait, just return tasks for continuation or later wait.
        return joinTasks(load_tasks, startup_tasks);
    }

    // NOTE: some tables can still be started up in the "loading" phase if they are required by dependencies during loading of other tables
    LOG_INFO(log, "Start synchronous loading of databases");
    waitLoad(TablesLoaderForegroundPoolId, load_tasks); // First prioritize, schedule and wait all the load table tasks
    LOG_INFO(log, "Start synchronous startup of databases");
    waitLoad(TablesLoaderForegroundPoolId, startup_tasks); // Only then prioritize, schedule and wait all the startup tasks
    return {};
}

static void loadSystemDatabaseImpl(ContextMutablePtr context, const String & database_name, const String & default_engine)
{
    String path = context->getPath() + "metadata/" + database_name;
    String metadata_file = path + ".sql";
    if (fs::exists(metadata_file + ".tmp"))
        fs::remove(metadata_file + ".tmp");

    if (fs::exists(fs::path(metadata_file)))
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, database_name, path, true);
    }
    else
    {
        checkUnsupportedVersion(context, database_name);
        /// Initialize system database manually
        String database_create_query = "CREATE DATABASE ";
        database_create_query += database_name;
        database_create_query += " ENGINE=";
        database_create_query += default_engine;
        executeCreateQuery(database_create_query, context, database_name, "<no file>", true, true);
    }
}

static void convertOrdinaryDatabaseToAtomic(LoggerPtr log, ContextMutablePtr context, const DatabasePtr & database,
                                            const String & name, const String tmp_name)
{
    /// It's kind of C++ script that creates temporary database with Atomic engine,
    /// moves all tables to it, drops old database and then renames new one to old name.

    String name_quoted = backQuoteIfNeed(name);
    String tmp_name_quoted = backQuoteIfNeed(tmp_name);

    LOG_INFO(log, "Will convert database {} from Ordinary to Atomic", name_quoted);

    String create_database_query = fmt::format("CREATE DATABASE IF NOT EXISTS {}", tmp_name_quoted);
    auto res = executeQuery(create_database_query, context, QueryFlags{ .internal = true }).second;
    executeTrivialBlockIO(res, context);
    res = {};
    auto tmp_database = DatabaseCatalog::instance().getDatabase(tmp_name);
    assert(tmp_database->getEngineName() == "Atomic");

    size_t num_tables = 0;
    std::unordered_set<String> inner_mv_tables;
    for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        ++num_tables;
        auto id = iterator->table()->getStorageID();
        id.database_name = tmp_name;
        /// We need some uuid for checkTableCanBeRenamed
        id.uuid = UUIDHelpers::generateV4();
        iterator->table()->checkTableCanBeRenamed(id);
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(iterator->table().get()))
        {
            /// We should not rename inner tables of MVs, because MVs are responsible for renaming it...
            if (mv->hasInnerTable())
                inner_mv_tables.emplace(mv->getTargetTable()->getStorageID().table_name);
        }
    }

    LOG_INFO(log, "Will move {} tables to {} (including {} inner tables of MVs)", num_tables, tmp_name_quoted, inner_mv_tables.size());

    for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        auto id = iterator->table()->getStorageID();
        if (inner_mv_tables.contains(id.table_name))
        {
            LOG_DEBUG(log, "Do not rename {}, because it will be renamed together with MV", id.getNameForLogs());
            continue;
        }

        String qualified_quoted_name = id.getFullTableName();
        id.database_name = tmp_name;
        String tmp_qualified_quoted_name = id.getFullTableName();

        String move_table_query = fmt::format("RENAME TABLE {} TO {}", qualified_quoted_name, tmp_qualified_quoted_name);
        res = executeQuery(move_table_query, context, QueryFlags{ .internal = true }).second;
        executeTrivialBlockIO(res, context);
        res = {};
    }

    LOG_INFO(log, "Moved all tables from {} to {}", name_quoted, tmp_name_quoted);

    if (!database->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database {} is not empty after moving tables", name_quoted);

    String drop_query = fmt::format("DROP DATABASE {}", name_quoted);
    context->setSetting("force_remove_data_recursively_on_drop", false);
    res = executeQuery(drop_query, context, QueryFlags{ .internal = true }).second;
    executeTrivialBlockIO(res, context);
    res = {};

    String rename_query = fmt::format("RENAME DATABASE {} TO {}", tmp_name_quoted, name_quoted);
    res = executeQuery(rename_query, context, QueryFlags{ .internal = true }).second;
    executeTrivialBlockIO(res, context);

    LOG_INFO(log, "Finished database engine conversion of {}", name_quoted);
}

/// Converts database with Ordinary engine to Atomic. Does nothing if database is not Ordinary.
/// Can be called only during server startup when there are no queries from users.
static void maybeConvertOrdinaryDatabaseToAtomic(ContextMutablePtr context, const String & database_name, LoadTaskPtrs * startup_tasks = nullptr)
{
    LoggerPtr log = getLogger("loadMetadata");

    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    if (!database)
    {
        LOG_WARNING(log, "Database {} not found (while trying to convert it from Ordinary to Atomic)", database_name);
        return;
    }

    if (database->getEngineName() != "Ordinary")
        return;

    const Strings permanently_detached_tables = database->getNamesOfPermanentlyDetachedTables();
    if (!permanently_detached_tables.empty())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot automatically convert database {} from Ordinary to Atomic, "
                        "because it contains permanently detached tables ({}) that were not loaded during startup. "
                        "Attach these tables, so server will load and convert them",
                        database_name, fmt::join(permanently_detached_tables, ", "));
    }

    String tmp_name = fmt::format(ORDINARY_TO_ATOMIC_PREFIX"{}.{}", database_name, thread_local_rng());

    try
    {
        if (startup_tasks) // NOTE: only for system database
        {
            /// It's not quite correct to run DDL queries while database is not started up.
            waitLoad(TablesLoaderForegroundPoolId, *startup_tasks);
            startup_tasks->clear();
        }

        auto local_context = Context::createCopy(context);

        /// We have to stop background operations that may lock table for share to avoid DEADLOCK_AVOIDED error
        /// on moving tables from Ordinary database. Server has not started to accept connections yet,
        /// so there are no user queries, only background operations
        LOG_INFO(log, "Will stop background operations to be able to rename tables in Ordinary database {}", database_name);
        static const auto actions_to_stop = {
            ActionLocks::PartsMerge, ActionLocks::PartsFetch, ActionLocks::PartsSend, ActionLocks::DistributedSend
        };
        for (const auto & action : actions_to_stop)
            InterpreterSystemQuery::startStopActionInDatabase(action, /* start */ false, database_name, database, context, log);

        local_context->setSetting("check_table_dependencies", false);
        local_context->setSetting("check_referential_table_dependencies", false);
        convertOrdinaryDatabaseToAtomic(log, local_context, database, database_name, tmp_name);

        LOG_INFO(log, "Will start background operations after renaming tables in database {}", database_name);
        for (const auto & action : actions_to_stop)
            InterpreterSystemQuery::startStopActionInDatabase(action, /* start */ true, database_name, database, context, log);

        auto new_database = DatabaseCatalog::instance().getDatabase(database_name);
        UUID db_uuid = new_database->getUUID();
        std::vector<UUID> tables_uuids;
        for (auto iterator = new_database->getTablesIterator(context); iterator->isValid(); iterator->next())
            tables_uuids.push_back(iterator->uuid());

        /// Reload database just in case (and update logger name)
        String detach_query = fmt::format("DETACH DATABASE {}", backQuoteIfNeed(database_name));
        auto res = executeQuery(detach_query, context, QueryFlags{ .internal = true }).second;
        executeTrivialBlockIO(res, context);
        res = {};

        /// Unlock UUID mapping, because it will be locked again on database reload.
        /// It's safe to do during metadata loading, because cleanup task is not started yet.
        DatabaseCatalog::instance().removeUUIDMappingFinally(db_uuid);
        for (const auto & uuid : tables_uuids)
            DatabaseCatalog::instance().removeUUIDMappingFinally(uuid);

        String path = context->getPath() + "metadata/" + escapeForFileName(database_name);
        /// force_restore_data is needed to re-create metadata symlinks
        loadDatabase(context, database_name, path, /* force_restore_data */ true);

        TablesLoader::Databases databases =
        {
            {database_name, DatabaseCatalog::instance().getDatabase(database_name)},
        };
        TablesLoader loader{context, databases, LoadingStrictnessLevel::FORCE_RESTORE};
        waitLoad(TablesLoaderForegroundPoolId, loader.loadTablesAsync());

        /// Startup tables if they were started before conversion and detach/attach
        if (startup_tasks) // NOTE: only for system database
            *startup_tasks = loader.startupTablesAsync(); // We have loaded old database(s), replace tasks to startup new database
        else
            // An old database was already loaded, so we should load new one as well
            waitLoad(TablesLoaderForegroundPoolId, loader.startupTablesAsync());
    }
    catch (Exception & e)
    {
        e.addMessage("Exception while trying to convert database {} from Ordinary to Atomic. It may be in some intermediate state."
            " You can finish conversion manually by moving the rest tables from {} to {} (using RENAME TABLE)"
            " and executing DROP DATABASE {} and RENAME DATABASE {} TO {}.",
            database_name, database_name, tmp_name, database_name, tmp_name, database_name);
        throw;
    }
}

void maybeConvertSystemDatabase(ContextMutablePtr context, LoadTaskPtrs & system_startup_tasks)
{
    /// TODO remove this check, convert system database unconditionally
    if (context->getSettingsRef()[Setting::allow_deprecated_database_ordinary])
        return;

    maybeConvertOrdinaryDatabaseToAtomic(context, DatabaseCatalog::SYSTEM_DATABASE, &system_startup_tasks);
}

void convertDatabasesEnginesIfNeed(const LoadTaskPtrs & load_metadata, ContextMutablePtr context)
{
    auto convert_flag_path = fs::path(context->getFlagsPath()) / "convert_ordinary_to_atomic";
    if (!fs::exists(convert_flag_path))
        return;

    LOG_INFO(getLogger("loadMetadata"), "Found convert_ordinary_to_atomic file in flags directory, "
                                                 "will try to convert all Ordinary databases to Atomic");

    // Wait for all table to be loaded and started
    waitLoad(TablesLoaderForegroundPoolId, load_metadata);

    for (const auto & [name, _] : DatabaseCatalog::instance().getDatabases())
        if (name != DatabaseCatalog::SYSTEM_DATABASE)
            maybeConvertOrdinaryDatabaseToAtomic(context, name);

    LOG_INFO(getLogger("loadMetadata"), "Conversion finished, removing convert_ordinary_to_atomic flag");
    fs::remove(convert_flag_path);
}

LoadTaskPtrs loadMetadataSystem(ContextMutablePtr context)
{
    loadSystemDatabaseImpl(context, DatabaseCatalog::SYSTEM_DATABASE, "Atomic");
    loadSystemDatabaseImpl(context, DatabaseCatalog::INFORMATION_SCHEMA, "Memory");
    loadSystemDatabaseImpl(context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, "Memory");

    TablesLoader::Databases databases =
    {
        {DatabaseCatalog::SYSTEM_DATABASE, DatabaseCatalog::instance().getSystemDatabase()},
        {DatabaseCatalog::INFORMATION_SCHEMA, DatabaseCatalog::instance().getDatabase(DatabaseCatalog::INFORMATION_SCHEMA)},
        {DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, DatabaseCatalog::instance().getDatabase(DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE)},
    };
    TablesLoader loader{context, databases, LoadingStrictnessLevel::FORCE_RESTORE};
    auto tasks = loader.loadTablesAsync();
    waitLoad(TablesLoaderForegroundPoolId, tasks);

    /// Will startup tables in system database after all databases are loaded.
    return loader.startupTablesAsync();
}

}
