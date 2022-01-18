#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Poco/File.h>
#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

DatabaseMemory::DatabaseMemory(const String & name_, const Context & context)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context)
    , data_path("data/" + escapeForFileName(database_name) + "/")
{}

void DatabaseMemory::createTable(
    const Context & /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    std::unique_lock lock{mutex};
    attachTableUnlocked(table_name, table, lock);
    create_queries.emplace(table_name, query);
}

void DatabaseMemory::dropTable(
    const Context & /*context*/,
    const String & table_name,
    bool /*no_delay*/)
{
    std::unique_lock lock{mutex};
    auto table = detachTableUnlocked(table_name, lock);
    try
    {
        table->drop();
        Poco::File table_data_dir{getTableDataPath(table_name)};
        if (table_data_dir.exists())
            table_data_dir.remove(true);
    }
    catch (...)
    {
        attachTableUnlocked(table_name, table, lock);
        throw;
    }
    table->is_dropped = true;
    create_queries.erase(table_name);
    UUID table_uuid = table->getStorageID().uuid;
    if (table_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(table_uuid);
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery() const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());
    create_query->storage->set(create_query->storage->engine, makeASTFunction(getEngineName()));
    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const String & table_name, const Context &, bool throw_on_error) const
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end() || !it->second)
    {
        if (throw_on_error)
            throw Exception("There is no metadata of table " + table_name + " in database " + database_name, ErrorCodes::UNKNOWN_TABLE);
        else
            return {};
    }
    return it->second->clone();
}

UUID DatabaseMemory::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, global_context))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseMemory::drop(const Context & context)
{
    /// Remove data on explicit DROP DATABASE
    std::filesystem::remove_all(context.getPath() + data_path);
}

}
