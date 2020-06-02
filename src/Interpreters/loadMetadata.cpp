#include <Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>

#include <Databases/DatabaseOrdinary.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/typeid_cast.h>


namespace DB
{

static void executeCreateQuery(
    const String & query,
    Context & context,
    const String & database,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name, 0, context.getSettingsRef().max_parser_depth);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.execute();
}


static void loadDatabase(
    Context & context,
    const String & database,
    const String & database_path,
    bool force_restore_data)
{
    String database_attach_query;
    String database_metadata_file = database_path + ".sql";

    if (Poco::File(database_metadata_file).exists())
    {
        /// There is .sql file with database creation statement.
        ReadBufferFromFile in(database_metadata_file, 1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else if (Poco::File(database_path).exists())
    {
        /// Database exists, but .sql file is absent. It's old-style Ordinary database (e.g. system or default)
        database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database) + " ENGINE = Ordinary";
    }
    else
    {
        /// It's first server run and we need create default and system databases.
        /// .sql file with database engine will be written for CREATE query.
        database_attach_query = "CREATE DATABASE " + backQuoteIfNeed(database);
    }

    executeCreateQuery(database_attach_query, context, database,
                       database_metadata_file, force_restore_data);
}


#define SYSTEM_DATABASE "system"


void loadMetadata(Context & context, const String & default_database_name)
{
    String path = context.getPath() + "metadata";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    /// Loop over databases.
    std::map<String, String> databases;
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
    {
        if (!it->isDirectory())
            continue;

        /// For '.svn', '.gitignore' directory and similar.
        if (it.name().at(0) == '.')
            continue;

        if (it.name() == SYSTEM_DATABASE)
            continue;

        databases.emplace(unescapeForFileName(it.name()), it.path().toString());
    }

    /// clickhouse-local creates DatabaseMemory as default database by itself
    /// For clickhouse-server we need create default database
    bool create_default_db_if_not_exists = !default_database_name.empty();
    bool metadata_dir_for_default_db_already_exists = databases.count(default_database_name);
    if (create_default_db_if_not_exists && !metadata_dir_for_default_db_already_exists)
        databases.emplace(default_database_name, path + "/" + escapeForFileName(default_database_name));

    for (const auto & [name, db_path] : databases)
        loadDatabase(context, name, db_path, has_force_restore_data_flag);

    if (has_force_restore_data_flag)
    {
        try
        {
            force_restore_data_flag_file.remove();
        }
        catch (...)
        {
            tryLogCurrentException("Load metadata", "Can't remove force restore file to enable data sanity checks");
        }
    }
}


void loadMetadataSystem(Context & context)
{
    String path = context.getPath() + "metadata/" SYSTEM_DATABASE;
    if (Poco::File(path).exists())
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, SYSTEM_DATABASE, path, true);
    }
    else
    {
        /// Initialize system database manually
        String global_path = context.getPath();
        Poco::File(global_path + "data/" SYSTEM_DATABASE).createDirectories();
        Poco::File(global_path + "metadata/" SYSTEM_DATABASE).createDirectories();

        auto system_database = std::make_shared<DatabaseOrdinary>(SYSTEM_DATABASE, global_path + "metadata/" SYSTEM_DATABASE "/", context);
        DatabaseCatalog::instance().attachDatabase(SYSTEM_DATABASE, system_database);
    }

}

}
