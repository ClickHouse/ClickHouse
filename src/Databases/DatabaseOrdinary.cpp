#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{
static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace
{
    void tryAttachTable(
        ContextMutablePtr context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database,
        const String & database_name,
        const String & metadata_path,
        bool has_force_restore_data_flag)
    {
        try
        {
            auto [table_name, table] = createTableFromAST(
                query,
                database_name,
                database.getTableDataPath(query),
                context,
                has_force_restore_data_flag);

            database.attachTable(table_name, table, database.getTableDataPath(query));
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach table " + backQuote(database_name) + "." + backQuote(query.table) + " from metadata file " + metadata_path
                + " from query " + serializeAST(query));
            throw;
        }
    }

    void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch)
    {
        if (processed % PRINT_MESSAGE_EACH_N_OBJECTS == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, "{}%", processed * 100.0 / total);
            watch.restart();
        }
    }
}


DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & metadata_path_, ContextPtr context_)
    : DatabaseOrdinary(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseOrdinary (" + name_ + ")", context_)
{
}

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr context_)
    : DatabaseOnDisk(name_, metadata_path_, data_path_, logger, context_)
{
}

void DatabaseOrdinary::loadStoredObjects(ContextMutablePtr local_context, bool has_force_restore_data_flag, bool /*force_attach*/)
{
    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    using FileNames = std::map<std::string, ASTPtr>;
    std::mutex file_names_mutex;
    FileNames file_names;

    size_t total_dictionaries = 0;

    auto process_metadata = [&file_names, &total_dictionaries, &file_names_mutex, this](
                                const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                create_query->database = database_name;

                if (fs::exists(full_path.string() + detached_suffix))
                {
                    /// FIXME: even if we don't load the table we can still mark the uuid of it as taken.
                    /// if (create_query->uuid != UUIDHelpers::Nil)
                    ///     DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);

                    const std::string table_name = file_name.substr(0, file_name.size() - 4);
                    LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));
                    return;
                }

                std::lock_guard lock{file_names_mutex};
                file_names[file_name] = ast;
                total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(local_context, process_metadata);

    size_t total_tables = file_names.size() - total_dictionaries;

    LOG_INFO(log, "Total {} tables and {} dictionaries.", total_tables, total_dictionaries);

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    ThreadPool pool;

    /// We must attach dictionaries before attaching tables
    /// because while we're attaching tables we may need to have some dictionaries attached
    /// (for example, dictionaries can be used in the default expressions for some tables).
    /// On the other hand we can attach any dictionary (even sourced from ClickHouse table)
    /// without having any tables attached. It is so because attaching of a dictionary means
    /// loading of its config only, it doesn't involve loading the dictionary itself.

    /// Attach dictionaries.
    for (const auto & name_with_query : file_names)
    {
        const auto & create_query = name_with_query.second->as<const ASTCreateQuery &>();

        if (create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]()
            {
                tryAttachTable(
                    local_context,
                    create_query,
                    *this,
                    database_name,
                    getMetadataPath() + name_with_query.first,
                    has_force_restore_data_flag);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
        }
    }

    pool.wait();

    /// Attach tables.
    for (const auto & name_with_query : file_names)
    {
        const auto & create_query = name_with_query.second->as<const ASTCreateQuery &>();

        if (!create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]()
            {
                tryAttachTable(
                    local_context,
                    create_query,
                    *this,
                    database_name,
                    getMetadataPath() + name_with_query.first,
                    has_force_restore_data_flag);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
        }
    }

    pool.wait();

    /// After all tables was basically initialized, startup them.
    startupTables(pool);
}


void DatabaseOrdinary::startupTables(ThreadPool & thread_pool)
{
    LOG_INFO(log, "Starting up tables.");

    const size_t total_tables = tables.size();
    if (!total_tables)
        return;

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto startup_one_table = [&](const StoragePtr & table)
    {
        table->startup();
        logAboutProgress(log, ++tables_processed, total_tables, watch);
    };


    try
    {
        for (const auto & table : tables)
            thread_pool.scheduleOrThrowOnError([&]() { startup_one_table(table.second); });
    }
    catch (...)
    {
        thread_pool.wait();
        throw;
    }
    thread_pool.wait();
}

void DatabaseOrdinary::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    String table_name = table_id.table_name;
    /// Read the definition of the table and replace the necessary parts with new ones.
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        statement.data(),
        statement.data() + statement.size(),
        "in file " + table_metadata_path,
        0,
        local_context->getSettingsRef().max_parser_depth);

    applyMetadataChangesToCreateQuery(ast, metadata);

    statement = getObjectDefinitionFromCreateQuery(ast);
    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (local_context->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, local_context);
}

void DatabaseOrdinary::commitAlterTable(const StorageID &, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & /*statement*/, ContextPtr /*query_context*/)
{
    try
    {
        /// rename atomically replaces the old file with the new one.
        fs::rename(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        throw;
    }
}

}
