#include <Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/TablesLoader.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

static void executeCreateQuery(
    const String & query,
    ContextMutablePtr context,
    const String & database,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser, query.data(), query.data() + query.size(), "in file " + file_name, 0, context->getSettingsRef().max_parser_depth);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceAttach(true);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
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
    else if (fs::exists(fs::path(database_path)))
    {
        /// TODO Remove this code (it's required for compatibility with versions older than 20.7)
        /// Database exists, but .sql file is absent. It's old-style Ordinary database (e.g. system or default)
        database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database) + " ENGINE = Ordinary";
    }
    else
    {
        /// It's first server run and we need create default and system databases.
        /// .sql file with database engine will be written for CREATE query.
        database_attach_query = "CREATE DATABASE " + backQuoteIfNeed(database);
    }

    try
    {
        executeCreateQuery(database_attach_query, context, database, database_metadata_file, force_restore_data);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading database {} from path {}", backQuote(database), database_path));
        throw;
    }
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

        const auto current_file = it->path().filename().string();
        if (!it->is_directory())
        {
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

            continue;
        }

        /// For '.svn', '.gitignore' directory and similar.
        if (current_file.at(0) == '.')
            continue;

        if (isSystemOrInformationSchema(current_file))
            continue;

        databases.emplace(unescapeForFileName(current_file), it->path().string());
    }

    /// clickhouse-local creates DatabaseMemory as default database by itself
    /// For clickhouse-server we need create default database
    bool create_default_db_if_not_exists = !default_database_name.empty();
    bool metadata_dir_for_default_db_already_exists = databases.count(default_database_name);
    if (create_default_db_if_not_exists && !metadata_dir_for_default_db_already_exists)
        databases.emplace(default_database_name, std::filesystem::path(path) / escapeForFileName(default_database_name));

    TablesLoader::Databases loaded_databases;
    for (const auto & [name, db_path] : databases)
    {
        loadDatabase(context, name, db_path, has_force_restore_data_flag);
        loaded_databases.insert({name, DatabaseCatalog::instance().getDatabase(name)});
    }

    TablesLoader loader{context, std::move(loaded_databases), has_force_restore_data_flag, /* force_attach */ true};
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
    if (fs::exists(fs::path(path)) || fs::exists(fs::path(metadata_file)))
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, database_name, path, true);
    }
    else
    {
        /// Initialize system database manually
        String database_create_query = "CREATE DATABASE ";
        database_create_query += database_name;
        database_create_query += " ENGINE=";
        database_create_query += default_engine;
        executeCreateQuery(database_create_query, context, database_name, "<no file>", true);
    }
}


void startupSystemTables()
{
    ThreadPool pool;
    DatabaseCatalog::instance().getSystemDatabase()->startupTables(pool, /* force_restore */ true, /* force_attach */ true);
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
    TablesLoader loader{context, databases, /* force_restore */ true, /* force_attach */ true};
    loader.loadTables();
    /// Will startup tables in system database after all databases are loaded.
}

}
