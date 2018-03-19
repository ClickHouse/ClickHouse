#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Stopwatch.h>
#include <common/ThreadPool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_METADATA_DOESNT_EXIST;
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;

namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name)
    {
        return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
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


DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & metadata_path_, const Context & context)
    : DatabaseMemory(name_), metadata_path(metadata_path_), data_path(context.getPath() + "data/" + escapeForFileName(name_) + "/")
{
    Poco::File(data_path).createDirectories();
}


void DatabaseOrdinary::loadTables(
    Context & context,
    ThreadPool * thread_pool,
    bool has_force_restore_data_flag)
{
    log = &Logger::get("DatabaseOrdinary (" + name + ")");

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

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(file_names.begin(), file_names.end());

    size_t total_tables = file_names.size();
    LOG_INFO(log, "Total " << total_tables << " tables.");

    String data_path = context.getPath() + "data/" + escapeForFileName(name) + "/";

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed {0};

    auto task_function = [&](FileNames::const_iterator begin, FileNames::const_iterator end)
    {
        for (auto it = begin; it != end; ++it)
        {
            const String & table = *it;

            /// Messages, so that it's not boring to wait for the server to load for a long time.
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0
                || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
                watch.restart();
            }

            loadTable(context, metadata_path, *this, name, data_path, table, has_force_restore_data_flag);
        }
    };

    const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
    size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;

    for (size_t i = 0; i < num_bunches; ++i)
    {
        auto begin = file_names.begin() + i * bunch_size;
        auto end = (i + 1 == num_bunches)
            ? file_names.end()
            : (file_names.begin() + (i + 1) * bunch_size);

        auto task = std::bind(task_function, begin, end);

        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();
    }

    if (thread_pool)
        thread_pool->wait();

    /// After all tables was basically initialized, startup them.
    startupTables(thread_pool);
}


void DatabaseOrdinary::startupTables(ThreadPool * thread_pool)
{
    LOG_INFO(log, "Starting up tables.");

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed {0};
    size_t total_tables = tables.size();

    auto task_function = [&](Tables::iterator begin, Tables::iterator end)
    {
        for (auto it = begin; it != end; ++it)
        {
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0
                || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
                watch.restart();
            }

            it->second->startup();
        }
    };

    const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
    size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;

    auto begin = tables.begin();
    for (size_t i = 0; i < num_bunches; ++i)
    {
        auto end = begin;

        if (i + 1 == num_bunches)
            end = tables.end();
        else
            std::advance(end, bunch_size);

        auto task = std::bind(task_function, begin, end);

        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();

        begin = end;
    }

    if (thread_pool)
        thread_pool->wait();
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
        std::lock_guard<std::mutex> lock(mutex);
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
            std::lock_guard<std::mutex> lock(mutex);
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


static ASTPtr getCreateQueryImpl(const String & path, const String & table_name)
{
    String table_metadata_path = detail::getTableMetadataPath(path, table_name);

    String query;
    {
        ReadBufferFromFile in(table_metadata_path, 4096);
        readStringUntilEOF(query, in);
    }

    ParserCreateQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "in file " + table_metadata_path);
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
    catch (const Exception & e)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Better diagnostics.
        throw Exception{e};
    }

    ASTPtr ast = getCreateQueryImpl(metadata_path, table_name);
    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.table = to_table_name;

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


ASTPtr DatabaseOrdinary::getCreateQuery(
    const Context & context,
    const String & table_name) const
{
    ASTPtr ast;
    try
    {
        ast = getCreateQueryImpl(metadata_path, table_name);
    }
    catch (const Exception & e)
    {
        /// Handle system.* tables for which there are no table.sql files
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST && tryGetTable(context, table_name) != nullptr)
            throw Exception("There is no CREATE TABLE query for table " + table_name, ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);

        throw;
    }

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = false;
    ast_create_query.database = name;

    return ast;
}


void DatabaseOrdinary::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv: tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard<std::mutex> lock(mutex);
    tables.clear();
}


void DatabaseOrdinary::drop()
{
    /// No additional removal actions are required.
}


void DatabaseOrdinary::alterTable(
    const Context & context,
    const String & name,
    const ColumnsDescription & columns,
    const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    String table_name_escaped = escapeForFileName(name);
    String table_metadata_tmp_path = metadata_path + "/" + table_name_escaped + ".sql.tmp";
    String table_metadata_path = metadata_path + "/" + table_name_escaped + ".sql";
    String statement;

    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, statement.data(), statement.data() + statement.size(), "in file " + table_metadata_path);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);

    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
    ast_create_query.replace(ast_create_query.columns, new_columns);

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

String DatabaseOrdinary::getDataPath() const
{
    return data_path;
}

String DatabaseOrdinary::getMetadataPath() const
{
    return metadata_path;
}

String DatabaseOrdinary::getTableMetadataPath(const String & table_name) const
{
    return detail::getTableMetadataPath(metadata_path, table_name);
}

}
