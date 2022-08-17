#include <Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/executeQuery.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/TablesLoader.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/typeid_cast.h>
#include <filesystem>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

static void executeCreateQuery(
    const String & query,
    ContextMutablePtr context,
    const String & database,
    const String & file_name,
    bool create,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser, query.data(), query.data() + query.size(), "in file " + file_name, 0, context->getSettingsRef().max_parser_depth);

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

void loadMetadata(ContextMutablePtr context, const String & default_database_name)
{
    Poco::Logger * log = &Poco::Logger::get("loadMetadata");

    String path = context->getPath() + "metadata";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    auto force_restore_data_flag_file = fs::path(context->getFlagsPath()) / "force_restore_data";
    bool has_force_restore_data_flag = fs::exists(force_restore_data_flag_file);

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

    auto mode = getLoadingStrictnessLevel(/* attach */ true, /* force_attach */ true, has_force_restore_data_flag);
    TablesLoader loader{context, std::move(loaded_databases), mode};
    loader.loadTables();
    loader.startupTables();

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
}

static void loadSystemDatabaseImpl(ContextMutablePtr context, const String & database_name, const String & default_engine)
{
    String path = context->getPath() + "metadata/" + database_name;
    String metadata_file = path + ".sql";
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

static void convertOrdinaryDatabaseToAtomic(ContextMutablePtr context, const DatabasePtr & database)
{
    /// It's kind of C++ script that creates temporary database with Atomic engine,
    /// moves all tables to it, drops old database and then renames new one to old name.

    Poco::Logger * log = &Poco::Logger::get("loadMetadata");

    String name = database->getDatabaseName();

    String tmp_name = fmt::format(".tmp_convert.{}.{}", name, thread_local_rng());

    String name_quoted = backQuoteIfNeed(name);
    String tmp_name_quoted = backQuoteIfNeed(tmp_name);

    LOG_INFO(log, "Will convert database {} from Ordinary to Atomic", name_quoted);

    String create_database_query = fmt::format("CREATE DATABASE IF NOT EXISTS {}", tmp_name_quoted);
    auto res = executeQuery(create_database_query, context, true);
    executeTrivialBlockIO(res, context);
    res = {};
    auto tmp_database = DatabaseCatalog::instance().getDatabase(tmp_name);
    assert(tmp_database->getEngineName() == "Atomic");

    size_t num_tables = 0;
    for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        ++num_tables;
        auto id = iterator->table()->getStorageID();
        id.database_name = tmp_name;
        iterator->table()->checkTableCanBeRenamed(id);
    }

    LOG_INFO(log, "Will move {} tables to {}", num_tables, tmp_name_quoted);

    for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        auto id = iterator->table()->getStorageID();
        String qualified_quoted_name = id.getFullTableName();
        id.database_name = tmp_name;
        String tmp_qualified_quoted_name = id.getFullTableName();

        String move_table_query = fmt::format("RENAME TABLE {} TO {}", qualified_quoted_name, tmp_qualified_quoted_name);
        res = executeQuery(move_table_query, context, true);
        executeTrivialBlockIO(res, context);
        res = {};
    }

    LOG_INFO(log, "Moved all tables from {} to {}", name_quoted, tmp_name_quoted);

    if (!database->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database {} is not empty after moving tables", name_quoted);

    String drop_query = fmt::format("DROP DATABASE {}", name_quoted);
    res = executeQuery(drop_query, context, true);
    executeTrivialBlockIO(res, context);
    res = {};

    String rename_query = fmt::format("RENAME DATABASE {} TO {}", tmp_name_quoted, name_quoted);
    res = executeQuery(rename_query, context, true);
    executeTrivialBlockIO(res, context);

    LOG_INFO(log, "Finished database engine conversion of {}", name_quoted);
}

void maybeConvertOrdinaryDatabaseToAtomic(ContextMutablePtr context, const DatabasePtr & database)
{
    if (database->getEngineName() != "Ordinary")
        return;

    if (context->getSettingsRef().allow_deprecated_database_ordinary)
        return;

    try
    {
        /// It's not quite correct to run DDL queries while database is not started up.
        startupSystemTables();

        auto local_context = Context::createCopy(context);
        local_context->setSetting("check_table_dependencies", false);
        convertOrdinaryDatabaseToAtomic(local_context, database);

        auto new_database = DatabaseCatalog::instance().getDatabase(DatabaseCatalog::SYSTEM_DATABASE);
        UUID db_uuid = new_database->getUUID();
        std::vector<UUID> tables_uuids;
        for (auto iterator = new_database->getTablesIterator(context); iterator->isValid(); iterator->next())
            tables_uuids.push_back(iterator->uuid());

        /// Reload database just in case (and update logger name)
        String detach_query = fmt::format("DETACH DATABASE {}", backQuoteIfNeed(DatabaseCatalog::SYSTEM_DATABASE));
        auto res = executeQuery(detach_query, context, true);
        executeTrivialBlockIO(res, context);
        res = {};

        /// Unlock UUID mapping, because it will be locked again on database reload.
        /// It's safe to do during metadata loading, because cleanup task is not started yet.
        DatabaseCatalog::instance().removeUUIDMappingFinally(db_uuid);
        for (const auto & uuid : tables_uuids)
            DatabaseCatalog::instance().removeUUIDMappingFinally(uuid);

        loadSystemDatabaseImpl(context, DatabaseCatalog::SYSTEM_DATABASE, "Atomic");
        TablesLoader::Databases databases =
        {
            {DatabaseCatalog::SYSTEM_DATABASE, DatabaseCatalog::instance().getSystemDatabase()},
        };
        TablesLoader loader{context, databases, LoadingStrictnessLevel::FORCE_RESTORE};
        loader.loadTables();

        /// Will startup tables usual way
    }
    catch (Exception & e)
    {
        e.addMessage("While trying to convert {} to Atomic", database->getDatabaseName());
        throw;
    }
}


void startupSystemTables()
{
    ThreadPool pool;
    DatabaseCatalog::instance().getSystemDatabase()->startupTables(pool, LoadingStrictnessLevel::FORCE_RESTORE);
}

void loadMetadataSystem(ContextMutablePtr context)
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
    loader.loadTables();
    /// Will startup tables in system database after all databases are loaded.
}

}
