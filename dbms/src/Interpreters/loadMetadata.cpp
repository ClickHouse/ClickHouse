#include <iomanip>
#include <thread>
#include <future>

#include <Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>

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

#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>


namespace DB
{

static void executeCreateQuery(
    const String & query,
    Context & context,
    const String & database,
    const String & file_name,
    ThreadPool * pool,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name, 0);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.attach = true;
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    if (pool)
        interpreter.setDatabaseLoadingThreadpool(*pool);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.execute();
}


static void loadDatabase(
    Context & context,
    const String & database,
    const String & database_path,
    ThreadPool * thread_pool,
    bool force_restore_data)
{
    /// There may exist .sql file with database creation statement.
    /// Or, if it is absent, then database with default engine is created.

    String database_attach_query;
    String database_metadata_file = database_path + ".sql";

    if (Poco::File(database_metadata_file).exists())
    {
        ReadBufferFromFile in(database_metadata_file, 1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else
        database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database);

    executeCreateQuery(database_attach_query, context, database, database_metadata_file, thread_pool, force_restore_data);
}


#define SYSTEM_DATABASE "system"


void loadMetadata(Context & context)
{
    String path = context.getPath() + "metadata";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    /// For parallel tables loading.
    ThreadPool thread_pool(SettingMaxThreads().getAutoValue());

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

    for (const auto & elem : databases)
        loadDatabase(context, elem.first, elem.second, &thread_pool, has_force_restore_data_flag);

    thread_pool.wait();

    if (has_force_restore_data_flag)
    {
        try
        {
            force_restore_data_flag_file.remove();
        }
        catch (...)
        {
            tryLogCurrentException("Load metadata", "Can't remove force restore file to enable data santity checks");
        }
    }
}


void loadMetadataSystem(Context & context)
{
    String path = context.getPath() + "metadata/" SYSTEM_DATABASE;
    if (Poco::File(path).exists())
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, SYSTEM_DATABASE, path, nullptr, true);
    }
    else
    {
        /// Initialize system database manually
        String global_path = context.getPath();
        Poco::File(global_path + "data/" SYSTEM_DATABASE).createDirectories();
        Poco::File(global_path + "metadata/" SYSTEM_DATABASE).createDirectories();

        auto system_database = std::make_shared<DatabaseOrdinary>(SYSTEM_DATABASE, global_path + "metadata/" SYSTEM_DATABASE, context);
        context.addDatabase(SYSTEM_DATABASE, system_database);
    }

}

}
