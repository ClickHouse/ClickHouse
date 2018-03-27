#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}

void DatabaseMemory::loadTables(
    Context & /*context*/,
    ThreadPool * /*thread_pool*/,
    bool /*has_force_restore_data_flag*/)
{
    log = &Logger::get("DatabaseMemory(" + name + ")");

    /// Nothing to load.
}

bool DatabaseMemory::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseMemory::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return {};
    return it->second;
}

DatabaseIteratorPtr DatabaseMemory::getIterator(const Context & /*context*/)
{
    std::lock_guard<std::mutex> lock(mutex);
    return std::make_unique<DatabaseSnaphotIterator>(tables);
}

bool DatabaseMemory::empty(const Context & /*context*/) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseMemory::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = tables.find(table_name);
        if (it == tables.end())
            throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second;
        tables.erase(it);
    }

    return res;
}

void DatabaseMemory::attachTable(const String & table_name, const StoragePtr & table)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (!tables.emplace(table_name, table).second)
        throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

void DatabaseMemory::createTable(
    const Context & /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & /*query*/)
{
    attachTable(table_name, table);
}

void DatabaseMemory::removeTable(
    const Context & /*context*/,
    const String & table_name)
{
    detachTable(table_name);
}

void DatabaseMemory::renameTable(
    const Context &,
    const String &,
    IDatabase &,
    const String &)
{
    throw Exception("DatabaseMemory: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseMemory::alterTable(
    const Context &,
    const String &,
    const ColumnsDescription &,
    const ASTModifier &)
{
    throw Exception("DatabaseMemory: alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseMemory::getTableMetadataModificationTime(
    const Context &,
    const String &)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseMemory::getCreateQuery(
    const Context &,
    const String &) const
{
    throw Exception("There is no CREATE TABLE query for DatabaseMemory tables", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

void DatabaseMemory::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

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

void DatabaseMemory::drop()
{
    /// Additional actions to delete database are not required.
}

}
