#include <iomanip>

#include <Poco/Event.h>
#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name)
    {
        return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
    }

    String getDatabaseMetadataPath(const String & base_path)
    {
        return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
    }

}

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
    Poco::File(data_path).createDirectories();
}


void DatabaseOrdinary::loadTables(
    Context & context,
    ThreadPool * thread_pool,
    bool has_force_restore_data_flag)
{
    using FileNames = std::vector<std::string>;
    FileNames file_names;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(metadata_path); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        /// There are files .sql.tmp - delete.
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
            file_names.push_back(dir_it.name());
        else
            throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + metadata_path,
                ErrorCodes::INCORRECT_FILE_NAME);
    }

    if (file_names.empty())
        return;

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(file_names.begin(), file_names.end());

    size_t total_tables = file_names.size();
    LOG_INFO(log, "Total " << total_tables << " tables.");

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed {0};
    Poco::Event all_tables_processed;
    ExceptionHandler exception_handler;

    auto task_function = [&](const String & table)
    {
        SCOPE_EXIT(
            if (++tables_processed == total_tables)
                all_tables_processed.set()
        );

        /// Messages, so that it's not boring to wait for the server to load for a long time.
        if ((tables_processed + 1) % PRINT_MESSAGE_EACH_N_TABLES == 0
            || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
            watch.restart();
        }

        loadTable(context, metadata_path, *this, name, data_path, table, has_force_restore_data_flag);
    };

    for (const auto & filename : file_names)
    {
        auto task = createExceptionHandledJob(std::bind(task_function, filename), exception_handler);

        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();
    }

    if (thread_pool)
        all_tables_processed.wait();

    exception_handler.throwIfException();

    /// After all tables was basically initialized, startup them.
    startupTables(thread_pool);
}


void DatabaseOrdinary::startupTables(ThreadPool * thread_pool)
{
    LOG_INFO(log, "Starting up tables.");

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed {0};
    size_t total_tables = tables.size();
    Poco::Event all_tables_processed;
    ExceptionHandler exception_handler;

    if (!total_tables)
        return;

    auto task_function = [&](const StoragePtr & table)
    {
        SCOPE_EXIT(
            if (++tables_processed == total_tables)
                all_tables_processed.set()
        );

        if ((tables_processed + 1) % PRINT_MESSAGE_EACH_N_TABLES == 0
            || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
        {
            LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
            watch.restart();
        }

        table->startup();
    };

    for (const auto & name_storage : tables)
    {
        auto task = createExceptionHandledJob(std::bind(task_function, name_storage.second), exception_handler);

        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();
    }

    if (thread_pool)
        all_tables_processed.wait();

    exception_handler.throwIfException();
}


void DatabaseOrdinary::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    {
        std::lock_guard lock(mutex);
        if (tables.find(table_name) != tables.end())
            throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    String table_metadata_path = getTableMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// Add a table to the map of known tables.
        {
            std::lock_guard lock(mutex);
            if (!tables.emplace(table_name, table).second)
                throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
        }

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}


void DatabaseOrdinary::removeTable(
    const Context & /*context*/,
    const String & table_name)
{
    StoragePtr res = detachTable(table_name);

    String table_metadata_path = getTableMetadataPath(table_name);

    try
    {
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        attachTable(table_name, res);
        throw;
    }
}

static ASTPtr getQueryFromMetadata(const String & metadata_path, bool throw_on_error = true)
{
    String query;

    try
    {
        ReadBufferFromFile in(metadata_path, 4096);
        readStringUntilEOF(query, in);
    }
    catch (const Exception & e)
    {
        if (!throw_on_error && e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return nullptr;
        else
            throw;
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
                             "in file " + metadata_path, /* allow_multi_statements = */ false, 0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

static ASTPtr getCreateQueryFromMetadata(const String & metadata_path, const String & database, bool throw_on_error)
{
    ASTPtr ast = getQueryFromMetadata(metadata_path, throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.database = database;
    }

    return ast;
}


void DatabaseOrdinary::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name)
{
    DatabaseOrdinary * to_database_concrete = typeid_cast<DatabaseOrdinary *>(&to_database);

    if (!to_database_concrete)
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        table->rename(context.getPath() + "/data/" + escapeForFileName(to_database_concrete->name) + "/",
            to_database_concrete->name,
            to_table_name);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Better diagnostics.
        throw Exception{Exception::CreateFromPoco, e};
    }

    ASTPtr ast = getQueryFromMetadata(detail::getTableMetadataPath(metadata_path, table_name));
    if (!ast)
        throw Exception("There is no metadata file for table " + table_name, ErrorCodes::FILE_DOESNT_EXIST);
    ast->as<ASTCreateQuery &>().table = to_table_name;

    /// NOTE Non-atomic.
    to_database_concrete->createTable(context, to_table_name, table, ast);
    removeTable(context, table_name);
}


time_t DatabaseOrdinary::getTableMetadataModificationTime(
    const Context & /*context*/,
    const String & table_name)
{
    String table_metadata_path = getTableMetadataPath(table_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
    {
        return meta_file.getLastModified().epochTime();
    }
    else
    {
        return static_cast<time_t>(0);
    }
}

ASTPtr DatabaseOrdinary::getCreateTableQueryImpl(const Context & context,
                                                 const String & table_name, bool throw_on_error) const
{
    ASTPtr ast;

    auto table_metadata_path = detail::getTableMetadataPath(metadata_path, table_name);
    ast = getCreateQueryFromMetadata(table_metadata_path, name, throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = tryGetTable(context, table_name) != nullptr;

        auto msg = has_table
                   ? "There is no CREATE TABLE query for table "
                   : "There is no metadata file for table ";

        throw Exception(msg + table_name, ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseOrdinary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseOrdinary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseOrdinary::getCreateDatabaseQuery(const Context & /*context*/) const
{
    ASTPtr ast;

    auto database_metadata_path = detail::getDatabaseMetadataPath(metadata_path);
    ast = getCreateQueryFromMetadata(database_metadata_path, name, true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(name) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}


void DatabaseOrdinary::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv: tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(mutex);
    tables.clear();
}

void DatabaseOrdinary::alterTable(
    const Context & context,
    const String & table_name,
    const ColumnsDescription & columns,
    const IndicesDescription & indices,
    const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    String table_name_escaped = escapeForFileName(table_name);
    String table_metadata_tmp_path = metadata_path + "/" + table_name_escaped + ".sql.tmp";
    String table_metadata_path = metadata_path + "/" + table_name_escaped + ".sql";
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

    ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);

    if (ast_create_query.columns_list->indices)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->indices, new_indices);
    else
        ast_create_query.columns_list->set(ast_create_query.columns_list->indices, new_indices);

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
    Poco::File(data_path).remove(false);
    Poco::File(metadata_path).remove(false);
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
    return detail::getTableMetadataPath(metadata_path, table_name);
}

}
