#include <iomanip>

#include <Core/Settings.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDictionary.h>
#include <Storages/StorageFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int CANNOT_CREATE_DICTIONARY_FROM_METADATA;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int CANNOT_PARSE_TEXT;
    extern const int EMPTY_LIST_OF_ATTRIBUTES_PASSED;
}


static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;


namespace
{
    void tryAttachTable(
        Context & context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database,
        const String & database_name,
        bool has_force_restore_data_flag)
    {
        assert(!query.is_dictionary);
        try
        {
            String table_name;
            StoragePtr table;
            std::tie(table_name, table)
                = createTableFromAST(query, database_name, database.getDataPath(), context, has_force_restore_data_flag);
            database.attachTable(table_name, table);
        }
        catch (const Exception & e)
        {
            throw Exception(
                "Cannot attach table '" + query.table + "' from query " + serializeAST(query)
                    + ". Error: " + DB::getCurrentExceptionMessage(true),
                e,
                DB::ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
        }
    }


    void tryAttachDictionary(
        Context & context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database)
    {
        assert(query.is_dictionary);
        try
        {
            database.attachDictionary(query.table, context);
        }
        catch (const Exception & e)
        {
            throw Exception(
                "Cannot create dictionary '" + query.table + "' from query " + serializeAST(query)
                    + ". Error: " + DB::getCurrentExceptionMessage(true),
                e,
                DB::ErrorCodes::CANNOT_CREATE_DICTIONARY_FROM_METADATA);
        }
    }


    void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch)
    {
        if (processed % PRINT_MESSAGE_EACH_N_OBJECTS == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, std::fixed << std::setprecision(2) << processed * 100.0 / total << "%");
            watch.restart();
        }
    }
}


DatabaseOrdinary::DatabaseOrdinary(String name_, const String & metadata_path_, const Context & context_)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , metadata_path(metadata_path_)
    , data_path("data/" + escapeForFileName(name) + "/")
    , log(&Logger::get("DatabaseOrdinary (" + name + ")"))
{
    Poco::File(context_.getPath() + getDataPath()).createDirectories();
}


void DatabaseOrdinary::loadStoredObjects(
    Context & context,
    bool has_force_restore_data_flag)
{

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    using FileNames = std::map<std::string, ASTPtr>;
    FileNames file_names;

    size_t total_dictionaries = 0;
    DatabaseOnDisk::iterateMetadataFiles(*this, log, context, [&file_names, &total_dictionaries, this](const String & file_name)
    {
        String full_path = metadata_path + "/" + file_name;
        try
        {
            auto ast = parseCreateQueryFromMetadataFile(full_path, log);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                file_names[file_name] = ast;
                total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (const Exception & e)
        {
            throw Exception(
                "Cannot parse definition from metadata file " + full_path + ". Error: " + DB::getCurrentExceptionMessage(true), e, ErrorCodes::CANNOT_PARSE_TEXT);
        }

    });

    size_t total_tables = file_names.size() - total_dictionaries;

    LOG_INFO(log, "Total " << total_tables << " tables and " << total_dictionaries << " dictionaries.");

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};
    std::atomic<size_t> dictionaries_processed{0};

    ThreadPool pool(SettingMaxThreads().getAutoValue());

    /// Attach tables.
    for (const auto & name_with_query : file_names)
    {
        const auto & create_query = name_with_query.second->as<const ASTCreateQuery &>();
        if (!create_query.is_dictionary)
            pool.scheduleOrThrowOnError([&]()
            {
                tryAttachTable(context, create_query, *this, getDatabaseName(), has_force_restore_data_flag);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
    }

    pool.wait();

    /// After all tables was basically initialized, startup them.
    startupTables(pool);

    /// Add database as repository
    auto dictionaries_repository = std::make_unique<ExternalLoaderDatabaseConfigRepository>(shared_from_this(), context);
    auto & external_loader = context.getExternalDictionariesLoader();
    external_loader.addConfigRepository(getDatabaseName(), std::move(dictionaries_repository));

    /// Attach dictionaries.
    for (const auto & name_with_query : file_names)
    {
        auto create_query = name_with_query.second->as<const ASTCreateQuery &>();
        if (create_query.is_dictionary)
        {
            tryAttachDictionary(context, create_query, *this);

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

    auto startupOneTable = [&](const StoragePtr & table)
    {
        table->startup();
        logAboutProgress(log, ++tables_processed, total_tables, watch);
    };

    try
    {
        for (const auto & table : tables)
            thread_pool.scheduleOrThrowOnError([&]() { startupOneTable(table.second); });
    }
    catch (...)
    {
        thread_pool.wait();
        throw;
    }
    thread_pool.wait();
}

void DatabaseOrdinary::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    DatabaseOnDisk::createTable(*this, context, table_name, table, query);
}

void DatabaseOrdinary::createDictionary(
    const Context & context,
    const String & dictionary_name,
    const ASTPtr & query)
{
    DatabaseOnDisk::createDictionary(*this, context, dictionary_name, query);
}

void DatabaseOrdinary::removeTable(
    const Context & context,
    const String & table_name)
{
    DatabaseOnDisk::removeTable(*this, context, table_name, log);
}

void DatabaseOrdinary::removeDictionary(
    const Context & context,
    const String & table_name)
{
    DatabaseOnDisk::removeDictionary(*this, context, table_name, log);
}

void DatabaseOrdinary::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    DatabaseOnDisk::renameTable<DatabaseOrdinary>(*this, context, table_name, to_database, to_table_name, lock);
}


time_t DatabaseOrdinary::getObjectMetadataModificationTime(
    const Context & /* context */,
    const String & table_name)
{
    return DatabaseOnDisk::getObjectMetadataModificationTime(*this, table_name);
}

ASTPtr DatabaseOrdinary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::getCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseOrdinary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::tryGetCreateTableQuery(*this, context, table_name);
}


ASTPtr DatabaseOrdinary::getCreateDictionaryQuery(const Context & context, const String & dictionary_name) const
{
    return DatabaseOnDisk::getCreateDictionaryQuery(*this, context, dictionary_name);
}

ASTPtr DatabaseOrdinary::tryGetCreateDictionaryQuery(const Context & context, const String & dictionary_name) const
{
    return DatabaseOnDisk::tryGetCreateTableQuery(*this, context, dictionary_name);
}

ASTPtr DatabaseOrdinary::getCreateDatabaseQuery(const Context & context) const
{
    return DatabaseOnDisk::getCreateDatabaseQuery(*this, context);
}

void DatabaseOrdinary::alterTable(
    const Context & context,
    const String & table_name,
    const ColumnsDescription & columns,
    const IndicesDescription & indices,
    const ConstraintsDescription & constraints,
    const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    String table_name_escaped = escapeForFileName(table_name);
    String table_metadata_tmp_path = getMetadataPath() + "/" + table_name_escaped + ".sql.tmp";
    String table_metadata_path = getMetadataPath() + "/" + table_name_escaped + ".sql";
    String statement;

    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, statement.data(), statement.data() + statement.size(), "in file " + table_metadata_path, 0);

    const auto & ast_create_query = ast->as<ASTCreateQuery &>();

    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
    ASTPtr new_indices = InterpreterCreateQuery::formatIndices(indices);
    ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(constraints);

    ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->indices, new_indices);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->constraints, new_constraints);

    if (storage_modifier)
        storage_modifier(*ast_create_query.storage);

    statement = getObjectDefinitionFromCreateQuery(ast);

    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

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


void DatabaseOrdinary::drop(const Context & context)
{
    DatabaseOnDisk::drop(*this, context);
}


String DatabaseOrdinary::getDataPath() const
{
    return data_path;
}

String DatabaseOrdinary::getMetadataPath() const
{
    return metadata_path;
}

String DatabaseOrdinary::getDatabaseName() const
{
    return name;
}

String DatabaseOrdinary::getObjectMetadataPath(const String & table_name) const
{
    return DatabaseOnDisk::getObjectMetadataPath(*this, table_name);
}

}
