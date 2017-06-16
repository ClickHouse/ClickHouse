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

#include <Storages/System/attachSystemTables.h>

#include <IO/ReadBufferFromFile.h>
#include <Common/escapeForFileName.h>

#include <Common/Stopwatch.h>


namespace DB
{

static void executeCreateQuery(
    const String & query,
    Context & context,
    const String & database,
    const String & file_name,
    ThreadPool & pool,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setDatabaseLoadingThreadpool(pool);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.execute();
}


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

        databases.emplace(unescapeForFileName(it.name()), it.path().toString());
    }

    static constexpr auto SYSTEM_DATABASE = "system";

    auto load_database = [&] (const String & database, const String & database_path)
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

        bool force_restore_data = has_force_restore_data_flag;

        /// For special system database, always restore data
        ///  to not fail on loading query_log and part_log tables, if they are corrupted.
        if (database == SYSTEM_DATABASE)
            force_restore_data = true;

        executeCreateQuery(database_attach_query, context, database, database_metadata_file, thread_pool, force_restore_data);
    };

    /// At first, load the system database
    auto it_system = databases.find(SYSTEM_DATABASE);
    if (it_system != databases.end())
    {
        load_database(it_system->first, it_system->second);
        databases.erase(it_system);
    }
    else
    {
        /// Initialize system database manually
        String global_path = context.getPath();
        Poco::File(global_path + "data/system").createDirectories();
        Poco::File(global_path + "metadata/system").createDirectories();

        auto system_database = std::make_shared<DatabaseOrdinary>("system", global_path + "metadata/system/");
        context.addDatabase("system", system_database);

        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        system_database->loadTables(context, nullptr, true);
    }

    /// After the system database is created, attach virtual system tables (in addition to query_lof and part_log)
    {
        DatabasePtr system_database = context.getDatabase("system");

        if (context.getApplicationType() == Context::ApplicationType::SERVER)
            attachSystemTablesServer(system_database, &context, context.hasZooKeeper());
        else if (context.getApplicationType() == Context::ApplicationType::LOCAL_SERVER)
            attachSystemTablesLocal(system_database);
    }

    /// Then, load remaining databases
    for (const auto & elem : databases)
        load_database(elem.first, elem.second);

    thread_pool.wait();

    if (has_force_restore_data_flag)
        force_restore_data_flag_file.remove();
}

}
