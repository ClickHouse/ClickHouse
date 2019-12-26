#include <Core/Settings.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>

#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <iomanip>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int LOGICAL_ERROR;
}



DatabaseLazy::DatabaseLazy(const String & name_, const String & metadata_path_, time_t expiration_time_, const Context & context_)
    : name(name_)
    , metadata_path(metadata_path_)
    , data_path("data/" + escapeForFileName(name) + "/")
    , expiration_time(expiration_time_)
    , log(&Logger::get("DatabaseLazy (" + name + ")"))
{
    Poco::File(context_.getPath() + getDataPath()).createDirectories();
}


void DatabaseLazy::loadStoredObjects(
    Context & context,
    bool /* has_force_restore_data_flag */)
{
    DatabaseOnDisk::iterateMetadataFiles(*this, log, context, [this](const String & file_name)
    {
        const std::string table_name = file_name.substr(0, file_name.size() - 4);
        attachTable(table_name, nullptr);
    });
}


void DatabaseLazy::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    SCOPE_EXIT({ clearExpiredTables(); });
    if (!endsWith(table->getName(), "Log"))
        throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
    DatabaseOnDisk::createTable(*this, context, table_name, table, query);

    /// DatabaseOnDisk::createTable renames file, so we need to get new metadata_modification_time.
    std::lock_guard lock(tables_mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        it->second.metadata_modification_time = DatabaseOnDisk::getObjectMetadataModificationTime(*this, table_name);
}


void DatabaseLazy::createDictionary(
    const Context & /*context*/,
    const String & /*dictionary_name*/,
    const ASTPtr & /*query*/)
{
    throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseLazy::removeTable(
    const Context & context,
    const String & table_name)
{
    SCOPE_EXIT({ clearExpiredTables(); });
    DatabaseOnDisk::removeTable(*this, context, table_name, log);
}

void DatabaseLazy::removeDictionary(
    const Context & /*context*/,
    const String & /*table_name*/)
{
    throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
}

ASTPtr DatabaseLazy::getCreateDictionaryQuery(
    const Context & /*context*/,
    const String & /*table_name*/) const
{
    throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
}

ASTPtr DatabaseLazy::tryGetCreateDictionaryQuery(const Context & /*context*/, const String & /*table_name*/) const
{
    return nullptr;
}

bool DatabaseLazy::isDictionaryExist(const Context & /*context*/, const String & /*table_name*/) const
{
    return false;
}


DatabaseDictionariesIteratorPtr DatabaseLazy::getDictionariesIterator(
    const Context & /*context*/,
    const FilterByNameFunction & /*filter_by_dictionary_name*/)
{
    return std::make_unique<DatabaseDictionariesSnapshotIterator>();
}

void DatabaseLazy::attachDictionary(
    const String & /*dictionary_name*/,
    const Context & /*context*/)
{
    throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseLazy::detachDictionary(const String & /*dictionary_name*/, const Context & /*context*/)
{
    throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseLazy::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    SCOPE_EXIT({ clearExpiredTables(); });
    DatabaseOnDisk::renameTable<DatabaseLazy>(*this, context, table_name, to_database, to_table_name, lock);
}


time_t DatabaseLazy::getObjectMetadataModificationTime(
    const Context & /* context */,
    const String & table_name)
{
    std::lock_guard lock(tables_mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        return it->second.metadata_modification_time;
    else
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}

ASTPtr DatabaseLazy::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::getCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseLazy::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::tryGetCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseLazy::getCreateDatabaseQuery(const Context & context) const
{
    return DatabaseOnDisk::getCreateDatabaseQuery(*this, context);
}

void DatabaseLazy::alterTable(
    const Context & /* context */,
    const String & /* table_name */,
    const ColumnsDescription & /* columns */,
    const IndicesDescription & /* indices */,
    const ConstraintsDescription & /* constraints */,
    const ASTModifier & /* storage_modifier */)
{
    SCOPE_EXIT({ clearExpiredTables(); });
    throw Exception("ALTER query is not supported for Lazy database.", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseLazy::drop(const Context & context)
{
    DatabaseOnDisk::drop(*this, context);
}

bool DatabaseLazy::isTableExist(
    const Context & /* context */,
    const String & table_name) const
{
    SCOPE_EXIT({ clearExpiredTables(); });
    std::lock_guard lock(tables_mutex);
    return tables_cache.find(table_name) != tables_cache.end();
}

StoragePtr DatabaseLazy::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    SCOPE_EXIT({ clearExpiredTables(); });
    {
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

        if (it->second.table)
        {
            cache_expiration_queue.erase(it->second.expiration_iterator);
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), it->second.last_touched, table_name);

            return it->second.table;
        }
    }

    return loadTable(context, table_name);
}

DatabaseTablesIteratorPtr DatabaseLazy::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
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
    LOG_DEBUG(log, "Attach table " << backQuote(table_name) << ".");
    std::lock_guard lock(tables_mutex);
    time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    auto [it, inserted] = tables_cache.emplace(std::piecewise_construct,
                              std::forward_as_tuple(table_name),
                              std::forward_as_tuple(table, current_time, DatabaseOnDisk::getObjectMetadataModificationTime(*this, table_name)));
    if (!inserted)
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), current_time, table_name);
}

StoragePtr DatabaseLazy::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        LOG_DEBUG(log, "Detach table " << backQuote(table_name) << ".");
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
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

String DatabaseLazy::getObjectMetadataPath(const String & table_name) const
{
    return DatabaseOnDisk::getObjectMetadataPath(*this, table_name);
}

StoragePtr DatabaseLazy::loadTable(const Context & context, const String & table_name) const
{
    SCOPE_EXIT({ clearExpiredTables(); });

    LOG_DEBUG(log, "Load table " << backQuote(table_name) << " to cache.");

    const String table_metadata_path = getMetadataPath() + "/" + escapeForFileName(table_name) + ".sql";

    try
    {
        String table_name_;
        StoragePtr table;
        Context context_copy(context); /// some tables can change context, but not LogTables

        auto ast = parseCreateQueryFromMetadataFile(table_metadata_path, log);
        if (ast)
            std::tie(table_name_, table) = createTableFromAST(
                ast->as<const ASTCreateQuery &>(), name, getDataPath(), context_copy, false);

        if (!ast || !endsWith(table->getName(), "Log"))
            throw Exception("Only *Log tables can be used with Lazy database engine.", ErrorCodes::LOGICAL_ERROR);
        {
            std::lock_guard lock(tables_mutex);
            auto it = tables_cache.find(table_name);
            if (it == tables_cache.end())
                throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

            if (it->second.expiration_iterator != cache_expiration_queue.end())
                cache_expiration_queue.erase(it->second.expiration_iterator);
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            it->second.expiration_iterator = cache_expiration_queue.emplace(cache_expiration_queue.end(), it->second.last_touched, table_name);

            return it->second.table = table;
        }
    }
    catch (const Exception & e)
    {
        throw Exception("Cannot create table from metadata file " + table_metadata_path + ". Error: " + DB::getCurrentExceptionMessage(true),
                e, DB::ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
    }
}

void DatabaseLazy::clearExpiredTables() const
{
    std::lock_guard lock(tables_mutex);
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
            LOG_DEBUG(log, "Drop table " << backQuote(it->first) << " from cache.");
            it->second.table.reset();
            expired_tables.erase(it->second.expiration_iterator);
            it->second.expiration_iterator = cache_expiration_queue.end();
        }
        else
        {
            LOG_DEBUG(log, "Table " << backQuote(it->first) << " is busy.");
            busy_tables.splice(busy_tables.end(), expired_tables, it->second.expiration_iterator);
        }
    }

    cache_expiration_queue.splice(cache_expiration_queue.begin(), busy_tables, busy_tables.begin(), busy_tables.end());
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
