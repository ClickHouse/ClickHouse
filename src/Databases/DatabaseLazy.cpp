#include <Core/Settings.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Common/escapeForFileName.h>

#include <base/logger_useful.h>
#include <base/scope_guard_safe.h>
#include <base/sort.h>
#include <iomanip>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}


DatabaseLazy::DatabaseLazy(const String & name_, const String & metadata_path_, time_t expiration_time_, ContextPtr context_)
    : DatabaseOnDisk(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseLazy (" + name_ + ")", context_)
    , expiration_time(expiration_time_)
{
}


void DatabaseLazy::loadStoredObjects(
    ContextMutablePtr local_context, bool /* force_restore */, bool /*force_attach*/, bool /* skip_startup_tables */)
{
    iterateMetadataFiles(local_context, [this, &local_context](const String & file_name)
    {
        const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));

        fs::path detached_permanently_flag = fs::path(getMetadataPath()) / (file_name + detached_suffix);
        if (fs::exists(detached_permanently_flag))
        {
            LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));
            return;
        }

        attachTable(local_context, table_name, nullptr, {});
    });
}


void DatabaseLazy::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });
    if (!endsWith(table->getName(), "Log"))
        throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
    DatabaseOnDisk::createTable(local_context, table_name, table, query);

    /// DatabaseOnDisk::createTable renames file, so we need to get new metadata_modification_time.
    std::lock_guard lock(mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        it->second.metadata_modification_time = DatabaseOnDisk::getObjectMetadataModificationTime(table_name);
}

void DatabaseLazy::dropTable(
    ContextPtr local_context,
    const String & table_name,
    bool no_delay)
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });
    DatabaseOnDisk::dropTable(local_context, table_name, no_delay);
}

void DatabaseLazy::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary)
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });
    DatabaseOnDisk::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
}


time_t DatabaseLazy::getObjectMetadataModificationTime(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        return it->second.metadata_modification_time;
    throw Exception("Table " + backQuote(database_name) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}

void DatabaseLazy::alterTable(
    ContextPtr /* context */,
    const StorageID & /*table_id*/,
    const StorageInMemoryMetadata & /* metadata */)
{
    clearExpiredTables();
    throw Exception("ALTER query is not supported for Lazy database.", ErrorCodes::UNSUPPORTED_METHOD);
}

bool DatabaseLazy::isTableExist(const String & table_name) const
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });
    std::lock_guard lock(mutex);
    return tables_cache.find(table_name) != tables_cache.end();
}

StoragePtr DatabaseLazy::tryGetTable(const String & table_name) const
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });
    {
        std::lock_guard lock(mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            return {};

        if (it->second.table)
        {
            cache_expiration_queue.erase(it->second.expiration_iterator);
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), it->second.last_touched, table_name);

            return it->second.table;
        }
    }

    return loadTable(table_name);
}

DatabaseTablesIteratorPtr DatabaseLazy::getTablesIterator(ContextPtr, const FilterByNameFunction & filter_by_table_name) const
{
    std::lock_guard lock(mutex);
    Strings filtered_tables;
    for (const auto & [table_name, cached_table] : tables_cache)
    {
        if (!filter_by_table_name || filter_by_table_name(table_name))
            filtered_tables.push_back(table_name);
    }
    ::sort(filtered_tables.begin(), filtered_tables.end());
    return std::make_unique<DatabaseLazyIterator>(*this, std::move(filtered_tables));
}

bool DatabaseLazy::empty() const
{
    return tables_cache.empty();
}

void DatabaseLazy::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & table, const String &)
{
    LOG_DEBUG(log, "Attach table {}.", backQuote(table_name));
    std::lock_guard lock(mutex);
    time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    auto [it, inserted] = tables_cache.emplace(std::piecewise_construct,
                              std::forward_as_tuple(table_name),
                              std::forward_as_tuple(table, current_time, DatabaseOnDisk::getObjectMetadataModificationTime(table_name)));
    if (!inserted)
        throw Exception("Table " + backQuote(database_name) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), current_time, table_name);
}

StoragePtr DatabaseLazy::detachTable(ContextPtr /* context */, const String & table_name)
{
    StoragePtr res;
    {
        LOG_DEBUG(log, "Detach table {}.", backQuote(table_name));
        std::lock_guard lock(mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + backQuote(database_name) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second.table;
        if (it->second.expiration_iterator != cache_expiration_queue.end())
            cache_expiration_queue.erase(it->second.expiration_iterator);
        tables_cache.erase(it);
    }
    return res;
}

void DatabaseLazy::shutdown()
{
    TablesCache tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables_cache;
    }

    for (const auto & kv : tables_snapshot)
    {
        if (kv.second.table)
            kv.second.table->flushAndShutdown();
    }

    std::lock_guard lock(mutex);
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

StoragePtr DatabaseLazy::loadTable(const String & table_name) const
{
    SCOPE_EXIT_MEMORY_SAFE({ clearExpiredTables(); });

    LOG_DEBUG(log, "Load table {} to cache.", backQuote(table_name));

    const String table_metadata_path = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + ".sql");

    try
    {
        StoragePtr table;
        auto context_copy = Context::createCopy(context); /// some tables can change context, but not LogTables

        auto ast = parseQueryFromMetadata(log, getContext(), table_metadata_path, /*throw_on_error*/ true, /*remove_empty*/false);
        if (ast)
        {
            const auto & ast_create = ast->as<const ASTCreateQuery &>();
            String table_data_path_relative = getTableDataPath(ast_create);
            table = createTableFromAST(ast_create, getDatabaseName(), table_data_path_relative, context_copy, false).second;
        }

        if (!ast || !endsWith(table->getName(), "Log"))
            throw Exception("Only *Log tables can be used with Lazy database engine.", ErrorCodes::LOGICAL_ERROR);

        table->startup();
        {
            std::lock_guard lock(mutex);
            auto it = tables_cache.find(table_name);
            if (it == tables_cache.end())
                throw Exception("Table " + backQuote(database_name) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

            if (it->second.expiration_iterator != cache_expiration_queue.end())
                cache_expiration_queue.erase(it->second.expiration_iterator);
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), it->second.last_touched, table_name);

            return it->second.table = table;
        }
    }
    catch (Exception & e)
    {
        e.addMessage("Cannot create table from metadata file " + table_metadata_path);
        throw;
    }
}

void DatabaseLazy::clearExpiredTables() const
try
{
    std::lock_guard lock(mutex);
    auto time_now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    CacheExpirationQueue expired_tables;
    auto expired_it = cache_expiration_queue.begin();
    while (expired_it != cache_expiration_queue.end() && (time_now - expired_it->last_touched) >= expiration_time)
        ++expired_it;

    expired_tables.splice(expired_tables.end(), cache_expiration_queue, cache_expiration_queue.begin(), expired_it);

    CacheExpirationQueue busy_tables;

    while (!expired_tables.empty())
    {
        String table_name = expired_tables.front().table_name;
        auto it = tables_cache.find(table_name);

        if (!it->second.table || it->second.table.unique())
        {
            LOG_DEBUG(log, "Drop table {} from cache.", backQuote(it->first));
            it->second.table.reset();
            expired_tables.erase(it->second.expiration_iterator);
            it->second.expiration_iterator = cache_expiration_queue.end();
        }
        else
        {
            LOG_DEBUG(log, "Table {} is busy.", backQuote(it->first));
            busy_tables.splice(busy_tables.end(), expired_tables, it->second.expiration_iterator);
        }
    }

    cache_expiration_queue.splice(cache_expiration_queue.begin(), busy_tables, busy_tables.begin(), busy_tables.end());
}
catch (...)
{
    tryLogCurrentException(log, __PRETTY_FUNCTION__);
}


DatabaseLazyIterator::DatabaseLazyIterator(const DatabaseLazy & database_, Strings && table_names_)
    : IDatabaseTablesIterator(database_.database_name)
    , database(database_)
    , table_names(std::move(table_names_))
    , iterator(table_names.begin())
    , current_storage(nullptr)
{
}

void DatabaseLazyIterator::next()
{
    current_storage.reset();
    ++iterator;
    while (isValid() && !database.isTableExist(*iterator))
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
        current_storage = database.tryGetTable(*iterator);
    return current_storage;
}

}
