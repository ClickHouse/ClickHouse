#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

DatabaseMemory::DatabaseMemory(const String & name_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")")
    , data_path("data/" + escapeForFileName(database_name) + "/")
{}

void DatabaseMemory::createTable(
    const Context & /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    std::lock_guard lock{mutex};
    attachTableUnlocked(table_name, table);
    create_queries.emplace(table_name, query);
}

void DatabaseMemory::dropTable(
    const Context & /*context*/,
    const String & table_name,
    bool /*no_delay*/)
{
    std::lock_guard lock{mutex};
    auto table = detachTableUnlocked(table_name);
    try
    {
        table->drop();
        Poco::File table_data_dir{getTableDataPath(table_name)};
        if (table_data_dir.exists())
            table_data_dir.remove(true);
    }
    catch (...)
    {
        attachTableUnlocked(table_name, table);
        throw;
    }
    table->is_dropped = true;
    create_queries.erase(table_name);
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery(const Context & /*context*/) const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());
    create_query->storage->set(create_query->storage->engine, makeASTFunction(getEngineName()));
    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const Context &, const String & table_name, bool throw_on_error) const
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end())
    {
        if (throw_on_error)
            throw Exception("There is no metadata of table " + table_name + " in database " + database_name, ErrorCodes::UNKNOWN_TABLE);
        else
            return {};
    }
    return it->second;
}

}
