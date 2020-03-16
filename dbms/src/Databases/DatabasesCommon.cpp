#include <Databases/DatabasesCommon.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
}

DatabaseWithOwnTablesBase::DatabaseWithOwnTablesBase(const String & name_, const String & logger)
        : IDatabase(name_), log(&Logger::get(logger))
{
}

bool DatabaseWithOwnTablesBase::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end() || dictionaries.find(table_name) != dictionaries.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    return {};
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesIterator(const Context & /*context*/, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables));
}

bool DatabaseWithOwnTablesBase::empty(const Context & /*context*/) const
{
    std::lock_guard lock(mutex);
    return tables.empty() && dictionaries.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    std::lock_guard lock(mutex);
    return detachTableUnlocked(table_name);
}

StoragePtr DatabaseWithOwnTablesBase::detachTableUnlocked(const String & table_name)
{
    StoragePtr res;
    if (dictionaries.count(table_name))
        throw Exception("Cannot detach dictionary " + database_name + "." + table_name + " as table, use DETACH DICTIONARY query.", ErrorCodes::UNKNOWN_TABLE);

    auto it = tables.find(table_name);
    if (it == tables.end())
        throw Exception("Table " + backQuote(database_name) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
    res = it->second;
    tables.erase(it);

    auto table_id = res->getStorageID();
    if (table_id.hasUUID())
    {
        /// For now it's the only database, which contains storages with UUID
        assert(getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE);
        DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
    }

    return res;
}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table, const String & relative_table_path)
{
    std::lock_guard lock(mutex);
    attachTableUnlocked(table_name, table, relative_table_path);
}

void DatabaseWithOwnTablesBase::attachTableUnlocked(const String & table_name, const StoragePtr & table, const String &)
{
    if (!tables.emplace(table_name, table).second)
        throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    auto table_id = table->getStorageID();
    if (table_id.hasUUID())
    {
        /// For now it's the only database, which contains storages with UUID
        assert(getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE);
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid, shared_from_this(), table);
    }
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(mutex);
    tables.clear();
    dictionaries.clear();
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        DatabaseWithOwnTablesBase::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
