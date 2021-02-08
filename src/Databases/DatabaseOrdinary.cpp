#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
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
#include <Poco/DirectoryIterator.h>
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

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    void tryAttachTable(
        Context & context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database,
        const String & database_name,
        const String & metadata_path,
        bool has_force_restore_data_flag)
    {
        assert(!query.is_dictionary);
        try
        {
            String table_name;
            StoragePtr table;
            std::tie(table_name, table)
                = createTableFromAST(query, database_name, database.getTableDataPath(query), context, has_force_restore_data_flag);
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


    void tryAttachDictionary(const ASTPtr & query, DatabaseOrdinary & database, const String & metadata_path, const Context & context)
    {
        auto & create_query = query->as<ASTCreateQuery &>();
        assert(create_query.is_dictionary);
        try
        {
            Poco::File meta_file(metadata_path);
            auto config = getDictionaryConfigurationFromAST(create_query, context, database.getDatabaseName());
            time_t modification_time = meta_file.getLastModified().epochTime();
            database.attachDictionary(create_query.table, DictionaryAttachInfo{query, config, modification_time});
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach dictionary " + backQuote(database.getDatabaseName()) + "." + backQuote(create_query.table)
                + " from metadata file " + metadata_path + " from query " + serializeAST(*query));
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


DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & metadata_path_, const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseOrdinary (" + name_ + ")", context_)
{
}

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context_)
    : DatabaseWithDictionaries(name_, metadata_path_, data_path_, logger, context_)
{
}

void DatabaseOrdinary::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool /*force_attach*/)
{
    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    using FileNames = std::map<std::string, ASTPtr>;
    std::mutex file_names_mutex;
    FileNames file_names;

    size_t total_dictionaries = 0;

    auto process_metadata = [&context, &file_names, &total_dictionaries, &file_names_mutex, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, context, full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                create_query->database = database_name;
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

    iterateMetadataFiles(context, process_metadata);

    size_t total_tables = file_names.size() - total_dictionaries;

    LOG_INFO(log, "Total {} tables and {} dictionaries.", total_tables, total_dictionaries);

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};
    std::atomic<size_t> dictionaries_processed{0};

    ThreadPool pool;

    /// Attach tables.
    for (const auto & name_with_query : file_names)
    {
        const auto & create_query = name_with_query.second->as<const ASTCreateQuery &>();
        if (!create_query.is_dictionary)
            pool.scheduleOrThrowOnError([&]()
            {
                tryAttachTable(
                    context,
                    create_query,
                    *this,
                    database_name,
                    getMetadataPath() + name_with_query.first,
                    has_force_restore_data_flag);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
    }

    pool.wait();

    /// After all tables was basically initialized, startup them.
    startupTables(pool);

    /// Attach dictionaries.
    for (const auto & [name, query] : file_names)
    {
        auto create_query = query->as<const ASTCreateQuery &>();
        if (create_query.is_dictionary)
        {
            tryAttachDictionary(query, *this, getMetadataPath() + name, context);

            /// Messages, so that it's not boring to wait for the server to load for a long time.
            logAboutProgress(log, ++dictionaries_processed, total_dictionaries, watch);
        }
    }
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

void DatabaseOrdinary::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
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
        context.getSettingsRef().max_parser_depth);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();

    bool has_structure = ast_create_query.columns_list && ast_create_query.columns_list->columns;
    if (ast_create_query.as_table_function && !has_structure)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot alter table {} because it was created AS table function"
                                                     " and doesn't have structure in metadata", backQuote(table_name));

    assert(has_structure);
    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(metadata.columns);
    ASTPtr new_indices = InterpreterCreateQuery::formatIndices(metadata.secondary_indices);
    ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(metadata.constraints);

    ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->indices, new_indices);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->constraints, new_constraints);

    if (metadata.select.select_query)
    {
        ast->replace(ast_create_query.select, metadata.select.select_query);
    }

    /// MaterializedView is one type of CREATE query without storage.
    if (ast_create_query.storage)
    {
        ASTStorage & storage_ast = *ast_create_query.storage;

        bool is_extended_storage_def
            = storage_ast.partition_by || storage_ast.primary_key || storage_ast.order_by || storage_ast.sample_by || storage_ast.settings;

        if (is_extended_storage_def)
        {
            if (metadata.sorting_key.definition_ast)
                storage_ast.set(storage_ast.order_by, metadata.sorting_key.definition_ast);

            if (metadata.primary_key.definition_ast)
                storage_ast.set(storage_ast.primary_key, metadata.primary_key.definition_ast);

            if (metadata.sampling_key.definition_ast)
                storage_ast.set(storage_ast.sample_by, metadata.sampling_key.definition_ast);

            if (metadata.table_ttl.definition_ast)
                storage_ast.set(storage_ast.ttl_table, metadata.table_ttl.definition_ast);
            else if (storage_ast.ttl_table != nullptr) /// TTL was removed
                storage_ast.ttl_table = nullptr;

            if (metadata.settings_changes)
                storage_ast.set(storage_ast.settings, metadata.settings_changes);
        }
    }

    statement = getObjectDefinitionFromCreateQuery(ast);
    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path);
}

void DatabaseOrdinary::commitAlterTable(const StorageID &, const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    try
    {
        /// rename atomically replaces the old file with the new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}

}
