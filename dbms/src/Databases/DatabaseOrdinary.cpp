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
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
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
}


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

static void loadTable(
    Context & context,
    const String & database_metadata_path,
    DatabaseOrdinary & database,
    const String & database_name,
    const String & database_data_path,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    Logger * log = &Logger::get("loadTable");

    const String table_metadata_path = database_metadata_path + "/" + file_name;

    String s;
    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(s, in);
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (s.empty())
    {
        LOG_ERROR(log, "File " << table_metadata_path << " is empty. Removing.");
        Poco::File(table_metadata_path).remove();
        return;
    }

    try
    {
        String table_name;
        StoragePtr table;
        std::tie(table_name, table) = createTableFromDefinition(
            s, database_name, database_data_path, context, has_force_restore_data_flag, "in file " + table_metadata_path);
        database.attachTable(table_name, table);
    }
    catch (const Exception & e)
    {
        throw Exception("Cannot create table from metadata file " + table_metadata_path + ", error: " + e.displayText() +
            ", stack trace:\n" + e.getStackTrace().toString(),
            ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
    }
}


DatabaseOrdinary::DatabaseOrdinary(String name_, const String & metadata_path_, const Context & context)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , metadata_path(metadata_path_)
    , data_path(context.getPath() + "data/" + escapeForFileName(name) + "/")
    , log(&Logger::get("DatabaseOrdinary (" + name + ")"))
{
    Poco::File(getDataPath()).createDirectories();
}


void DatabaseOrdinary::loadTables(
    Context & context,
    bool has_force_restore_data_flag)
{
    using FileNames = std::vector<std::string>;
    FileNames file_names;

    DatabaseOnDisk::iterateTableFiles(*this, log, [&file_names](const String & file_name)
    {
        file_names.push_back(file_name);
    });

    if (file_names.empty())
        return;

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(file_names.begin(), file_names.end());

    const size_t total_tables = file_names.size();
    LOG_INFO(log, "Total " << total_tables << " tables.");

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed {0};

    auto loadOneTable = [&](const String & table)
    {
        loadTable(context, getMetadataPath(), *this, getDatabaseName(), getDataPath(), table, has_force_restore_data_flag);

        /// Messages, so that it's not boring to wait for the server to load for a long time.
        if (++tables_processed % PRINT_MESSAGE_EACH_N_TABLES == 0
            || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
            watch.restart();
        }
    };

    ThreadPool pool(SettingMaxThreads().getAutoValue());

    for (const auto & file_name : file_names)
    {
        pool.scheduleOrThrowOnError([&]() { loadOneTable(file_name); });
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
    std::atomic<size_t> tables_processed {0};

    auto startupOneTable = [&](const StoragePtr & table)
    {
        table->startup();

        if (++tables_processed % PRINT_MESSAGE_EACH_N_TABLES == 0
            || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
            watch.restart();
        }
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


void DatabaseOrdinary::removeTable(
    const Context & context,
    const String & table_name)
{
    DatabaseOnDisk::removeTable(*this, context, table_name, log);
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


time_t DatabaseOrdinary::getTableMetadataModificationTime(
    const Context & /* context */,
    const String & table_name)
{
    return DatabaseOnDisk::getTableMetadataModificationTime(*this, table_name);
}

ASTPtr DatabaseOrdinary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::getCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseOrdinary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::tryGetCreateTableQuery(*this, context, table_name);
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

    if (ast_create_query.columns_list->indices)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->indices, new_indices);
    else
        ast_create_query.columns_list->set(ast_create_query.columns_list->indices, new_indices);

    if (ast_create_query.columns_list->constraints)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->constraints, new_constraints);
    else
        ast_create_query.columns_list->set(ast_create_query.columns_list->constraints, new_constraints);

    if (storage_modifier)
        storage_modifier(*ast_create_query.storage);

    statement = getTableDefinitionFromCreateQuery(ast);

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


void DatabaseOrdinary::drop()
{
    DatabaseOnDisk::drop(*this);
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

String DatabaseOrdinary::getTableMetadataPath(const String & table_name) const
{
    return detail::getTableMetadataPath(getMetadataPath(), table_name);
}

}
