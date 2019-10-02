#include <iomanip>

#include <Core/Settings.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseLazy.h>
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
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}


static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace detail
{
    extern String getTableMetadataPath(const String & base_path, const String & table_name);
    extern String getDatabaseMetadataPath(const String & base_path);
}

static size_t getLastModifiedEpochTime(const String & table_metadata_path) {
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

DatabaseLazy::DatabaseLazy(const String & name_, const String & metadata_path_, time_t expiration_time_, const Context & context)
    : name(name_)
    , metadata_path(metadata_path_)
    , data_path(context.getPath() + "data/" + escapeForFileName(name) + "/")
    , expiration_time(expiration_time_)
    , log(&Logger::get("DatabaseLazy (" + name + ")"))
{
    Poco::File(getDataPath()).createDirectories();
}


void DatabaseLazy::loadTables(
    Context & /* context */,
    bool /* has_force_restore_data_flag */)
{
    iterateTableFiles([this](const Poco::DirectoryIterator & dir_it) {
        const std::string table_name = dir_it.name().substr(0, dir_it.name().size() - 4);
        attachTable(table_name, nullptr);
    });
}


void DatabaseLazy::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    clearExpiredTables();
    const auto & settings = context.getSettingsRef();

    if (!endsWith(table->getName(), "Log"))
        throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isTableExist(context, table_name))
        throw Exception("Table " + getDatabaseName() + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

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
        attachTable(table_name, table);

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


void DatabaseLazy::removeTable(
    const Context & /*context*/,
    const String & table_name)
{
    clearExpiredTables();
    StoragePtr res = detachTable(table_name);

    String table_metadata_path = getTableMetadataPath(table_name);

    try
    {
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        try
        {
            Poco::File(table_metadata_path + ".tmp_drop").remove();
            return;
        }
        catch (...)
        {
            LOG_WARNING(log, getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        }
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


void DatabaseLazy::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    clearExpiredTables();
    DatabaseLazy * to_database_concrete = typeid_cast<DatabaseLazy *>(&to_database);

    if (!to_database_concrete)
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        table->rename(context.getPath() + "/data/" + escapeForFileName(to_database_concrete->getDatabaseName()) + "/",
            to_database_concrete->getDatabaseName(),
            to_table_name, lock);
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

    ASTPtr ast = getQueryFromMetadata(detail::getTableMetadataPath(getMetadataPath(), table_name));
    if (!ast)
        throw Exception("There is no metadata file for table " + table_name, ErrorCodes::FILE_DOESNT_EXIST);
    ast->as<ASTCreateQuery &>().table = to_table_name;

    /// NOTE Non-atomic.
    to_database_concrete->createTable(context, to_table_name, table, ast);
    removeTable(context, table_name);
}


time_t DatabaseLazy::getTableMetadataModificationTime(
    const Context & /*context*/,
    const String & table_name)
{
    std::lock_guard lock(tables_mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        return it->second.metadata_modification_time;
    else
        throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}

ASTPtr DatabaseLazy::getCreateTableQueryImpl(const Context & context,
                                                 const String & table_name, bool throw_on_error) const
{
    ASTPtr ast;

    auto table_metadata_path = detail::getTableMetadataPath(getMetadataPath(), table_name);
    ast = getCreateQueryFromMetadata(table_metadata_path, getDatabaseName(), throw_on_error);
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

ASTPtr DatabaseLazy::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseLazy::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseLazy::getCreateDatabaseQuery(const Context & /*context*/) const
{
    ASTPtr ast;

    auto database_metadata_path = detail::getDatabaseMetadataPath(getMetadataPath());
    ast = getCreateQueryFromMetadata(database_metadata_path, getDatabaseName(), true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Lazy";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}

void DatabaseLazy::alterTable(
    const Context & /* context */,
    const String & /* table_name */,
    const ColumnsDescription & /* columns */,
    const IndicesDescription & /* indices */,
    const ConstraintsDescription & /* constraints */,
    const ASTModifier & /* storage_modifier */)
{
    clearExpiredTables();
    throw Exception("ALTER query is not supported for Lazy database.", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseLazy::drop()
{
    Poco::File(getDataPath()).remove(false);
    Poco::File(getMetadataPath()).remove(false);
}

bool DatabaseLazy::isTableExist(
    const Context & /* context */,
    const String & table_name) const
{
    clearExpiredTables();
    std::lock_guard lock(tables_mutex);
    return tables_cache.find(table_name) != tables_cache.end();
}

StoragePtr DatabaseLazy::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    clearExpiredTables();
    {
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

        if (it->second.table)
        {
            cache_expiration_queue.erase({it->second.last_touched, table_name});
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            cache_expiration_queue.emplace(it->second.last_touched, table_name);

            return it->second.table;
        }
    }
    
    return loadTable(context, table_name);
}

DatabaseIteratorPtr DatabaseLazy::getIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(tables_mutex);
    Strings filtered_tables;
    for (const auto & [table_name, cached_table] : tables_cache)
    {
        if (!filter_by_table_name || filter_by_table_name(table_name))
            filtered_tables.push_back(table_name);
    }
    std::sort(filtered_tables.begin(), filtered_tables.end());
    return std::make_unique<DatabaseLazyIterator>(*this, context, std::move(filtered_tables));
}

bool DatabaseLazy::empty(const Context & /* context */) const
{
    return tables_cache.empty();
}

void DatabaseLazy::attachTable(const String & table_name, const StoragePtr & table)
{
    LOG_DEBUG(log, "attach table" << table_name);
    std::lock_guard lock(tables_mutex);
    time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    if (!tables_cache.emplace(std::piecewise_construct,
                              std::forward_as_tuple(table_name),
                              std::forward_as_tuple(table,
                                                    current_time,
                                                    getLastModifiedEpochTime(getTableMetadataPath(table_name)))).second)
        throw Exception("Table " + getDatabaseName() + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    if (!cache_expiration_queue.emplace(current_time, table_name).second)
        throw Exception("Failed to insert element to expiration queue.", ErrorCodes::LOGICAL_ERROR);
}

StoragePtr DatabaseLazy::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        LOG_DEBUG(log, "detach table" << table_name);
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second.table;
        cache_expiration_queue.erase({it->second.last_touched, table_name});
        tables_cache.erase(it);
    }
    return res;
}

void DatabaseLazy::shutdown()
{
    TablesCache tables_snapshot;
    {
        std::lock_guard lock(tables_mutex);
        tables_snapshot = tables_cache;
    }

    for (const auto & kv : tables_snapshot)
    {
        if (kv.second.table)
            kv.second.table->shutdown();
    }

    std::lock_guard lock(tables_mutex);
    tables_cache.clear();
}

DatabaseLazy::~DatabaseLazy()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

String DatabaseLazy::getDataPath() const
{
    return data_path;
}

String DatabaseLazy::getMetadataPath() const
{
    return metadata_path;
}

String DatabaseLazy::getDatabaseName() const
{
    return name;
}

String DatabaseLazy::getTableMetadataPath(const String & table_name) const
{
    return detail::getTableMetadataPath(getMetadataPath(), table_name);
}

void DatabaseLazy::iterateTableFiles(const IteratingFunction & iterating_function) const
{
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(getMetadataPath()); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        // There are files that we tried to delete previously
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        if (endsWith(dir_it.name(), tmp_drop_ext))
        {
            const std::string table_name = dir_it.name().substr(0, dir_it.name().size() - strlen(tmp_drop_ext));
            if (Poco::File(getDataPath() + '/' + table_name).exists())
            {
                Poco::File(dir_it->path()).renameTo(table_name + ".sql");
                LOG_WARNING(log, "Table " << backQuote(table_name) << " was not dropped previously");
            }
            else
            {
                LOG_INFO(log, "Removing file " << dir_it->path());
                Poco::File(dir_it->path()).remove();
            }
            continue;
        }

        /// There are files .sql.tmp - delete
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
        {
            iterating_function(dir_it);
        }
        else
            throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + metadata_path,
                ErrorCodes::INCORRECT_FILE_NAME);
    }
}

StoragePtr DatabaseLazy::loadTable(const Context & context, const String & table_name) const
{
    clearExpiredTables();

    LOG_DEBUG(log, "Load table '" << table_name << "' to cache.");

    const String table_metadata_path = getMetadataPath() + "/" + table_name + ".sql";

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
        // TODO: exception
        LOG_ERROR(log, "loadTable: File " << table_metadata_path << " is empty. Removing.");
        Poco::File(table_metadata_path).remove();
        return nullptr;
    }

    try
    {
        String table_name_;
        StoragePtr table;
        Context context_copy(context); /// some tables can change context, but not LogTables
        std::tie(table_name_, table) = createTableFromDefinition(
            s, name, getDataPath(), context_copy, false, "in file " + table_metadata_path);
        if (!endsWith(table->getName(), "Log"))
            throw Exception("Only *Log tables can be used with Lazy database engine.", ErrorCodes::LOGICAL_ERROR);
        {
            std::lock_guard lock(tables_mutex);
            auto it = tables_cache.find(table_name);
            if (it == tables_cache.end())
                throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

            cache_expiration_queue.erase({it->second.last_touched, table_name});
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            cache_expiration_queue.emplace(it->second.last_touched, table_name);

            return it->second.table = table;
        }
    }
    catch (const Exception & e)
    {
        throw Exception("Cannot create table from metadata file " + table_metadata_path + ", error: " + e.displayText() +
            ", stack trace:\n" + e.getStackTrace().toString(),
            ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
    }
}

void DatabaseLazy::clearExpiredTables() const
{
    std::lock_guard lock(tables_mutex);
    while (!cache_expiration_queue.empty() &&
           (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - cache_expiration_queue.begin()->last_touched) >= expiration_time)
    {
        auto it = tables_cache.find(cache_expiration_queue.begin()->table_name);
        LOG_DEBUG(log, "Drop table '" << it->first << "' from cache.");
        /// Table can be already removed by detachTable.
        if (it != tables_cache.end())
            it->second.table.reset();
        cache_expiration_queue.erase(cache_expiration_queue.begin());
    }
}


DatabaseLazyIterator::DatabaseLazyIterator(DatabaseLazy & database_, const Context & context_, Strings && table_names_)
    : database(database_)
    , table_names(std::move(table_names_))
    , context(context_)
    , iterator(table_names.begin())
    , current_storage(nullptr)
{
}

void DatabaseLazyIterator::next()
{
    current_storage.reset();
    ++iterator;
    while (isValid() && !database.isTableExist(context, *iterator))
        ++iterator;
}

bool DatabaseLazyIterator::isValid() const
{
    return iterator != table_names.end();
}

const String & DatabaseLazyIterator::name() const
{
    return *iterator;
}

const StoragePtr & DatabaseLazyIterator::table() const
{
    if (!current_storage)
        current_storage = database.tryGetTable(context, *iterator);
    return current_storage;
}

}
