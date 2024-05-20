#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/TablesLoader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
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
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric DatabaseOrdinaryThreads;
    extern const Metric DatabaseOrdinaryThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace
{
    void tryAttachTable(
        ContextMutablePtr context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database,
        const String & database_name,
        const String & metadata_path,
        bool force_restore)
    {
        try
        {
            auto [table_name, table] = createTableFromAST(
                query,
                database_name,
                database.getTableDataPath(query),
                context,
                force_restore);

            database.attachTable(context, table_name, table, database.getTableDataPath(query));
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach table " + backQuote(database_name) + "." + backQuote(query.getTable()) + " from metadata file " + metadata_path
                + " from query " + serializeAST(query));
            throw;
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

void DatabaseOrdinary::loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel mode)
{
    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */

    ParsedTablesMetadata metadata;
    bool force_attach = LoadingStrictnessLevel::FORCE_ATTACH <= mode;
    loadTablesMetadata(local_context, metadata, force_attach);

    size_t total_tables = metadata.parsed_tables.size() - metadata.total_dictionaries;

    AtomicStopwatch watch;
    std::atomic<size_t> dictionaries_processed{0};
    std::atomic<size_t> tables_processed{0};

    ThreadPool pool(CurrentMetrics::DatabaseOrdinaryThreads, CurrentMetrics::DatabaseOrdinaryThreadsActive);

    /// We must attach dictionaries before attaching tables
    /// because while we're attaching tables we may need to have some dictionaries attached
    /// (for example, dictionaries can be used in the default expressions for some tables).
    /// On the other hand we can attach any dictionary (even sourced from ClickHouse table)
    /// without having any tables attached. It is so because attaching of a dictionary means
    /// loading of its config only, it doesn't involve loading the dictionary itself.

    /// Attach dictionaries.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & path = name_with_path_and_query.second.path;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]()
            {
                loadTableFromMetadata(local_context, path, name, ast, mode);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++dictionaries_processed, metadata.total_dictionaries, watch);
            });
        }
    }

    pool.wait();

    /// Attach tables.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & path = name_with_path_and_query.second.path;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (!create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]()
            {
                loadTableFromMetadata(local_context, path, name, ast, mode);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
        }
    }

    pool.wait();
}

void DatabaseOrdinary::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto process_metadata = [&metadata, is_startup, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                FunctionNameNormalizer().visit(ast.get());
                auto * create_query = ast->as<ASTCreateQuery>();
                /// NOTE No concurrent writes are possible during database loading
                create_query->setDatabase(TSA_SUPPRESS_WARNING_FOR_READ(database_name));

                /// Even if we don't load the table we can still mark the uuid of it as taken.
                if (create_query->uuid != UUIDHelpers::Nil)
                {
                    /// A bit tricky way to distinguish ATTACH DATABASE and server startup (actually it's "force_attach" flag).
                    if (is_startup)
                    {
                        /// Server is starting up. Lock UUID used by permanently detached table.
                        DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);
                    }
                    else if (!DatabaseCatalog::instance().hasUUIDMapping(create_query->uuid))
                    {
                        /// It's ATTACH DATABASE. UUID for permanently detached table must be already locked.
                        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
                        if (getEngineName() != "MaterializedPostgreSQL")
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create_query->uuid);
                    }
                }

                if (fs::exists(full_path.string() + detached_suffix))
                {
                    const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));
                    permanently_detached_tables.push_back(table_name);
                    LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));
                    return;
                }

                QualifiedTableName qualified_name{TSA_SUPPRESS_WARNING_FOR_READ(database_name), create_query->getTable()};

                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast};
                metadata.total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(local_context, process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "Metadata processed, database {} has {} tables and {} dictionaries in total.",
             TSA_SUPPRESS_WARNING_FOR_READ(database_name), tables_in_database, dictionaries_in_database);
}

void DatabaseOrdinary::loadTableFromMetadata(ContextMutablePtr local_context, const String & file_path, const QualifiedTableName & name, const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    assert(name.database == TSA_SUPPRESS_WARNING_FOR_READ(database_name));
    const auto & create_query = ast->as<const ASTCreateQuery &>();

    tryAttachTable(
        local_context,
        create_query,
        *this,
        name.database,
        file_path, LoadingStrictnessLevel::FORCE_RESTORE <= mode);
}

void DatabaseOrdinary::startupTables(ThreadPool & thread_pool, LoadingStrictnessLevel /*mode*/)
{
    LOG_INFO(log, "Starting up tables.");

    /// NOTE No concurrent writes are possible during database loading
    const size_t total_tables = TSA_SUPPRESS_WARNING_FOR_READ(tables).size();
    if (!total_tables)
        return;

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto startup_one_table = [&](const StoragePtr & table)
    {
        /// Since startup() method can use physical paths on disk we don't allow any exclusive actions (rename, drop so on)
        /// until startup finished.
        auto table_lock_holder = table->lockForShare(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);
        table->startup();
        logAboutProgress(log, ++tables_processed, total_tables, watch);
    };


    try
    {
        for (const auto & table : TSA_SUPPRESS_WARNING_FOR_READ(tables))
            thread_pool.scheduleOrThrowOnError([&]() { startup_one_table(table.second); });
    }
    catch (...)
    {
        /// We have to wait for jobs to finish here, because job function has reference to variables on the stack of current thread.
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

    /// The create query of the table has been just changed, we need to update dependencies too.
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies, loading_dependencies);

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
