#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Poco/File.h>


namespace DB
{

DatabaseMemory::DatabaseMemory(const String & name_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")")
    , data_path("data/" + escapeForFileName(database_name) + "/")
{}

void DatabaseMemory::createTable(
    const Context & /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & /*query*/)
{
    attachTable(table_name, table);
}

void DatabaseMemory::dropTable(
    const Context & /*context*/,
    const String & table_name)
{
    auto table = detachTable(table_name);
    try
    {
        table->drop();
        Poco::File table_data_dir{getTableDataPath(table_name)};
        if (table_data_dir.exists())
            table_data_dir.remove(true);
    }
    catch (...)
    {
        attachTable(table_name, table);
    }
    table->is_dropped = true;
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery(const Context & /*context*/) const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());
    create_query->storage->set(create_query->storage->engine, makeASTFunction(getEngineName()));
    return create_query;
}

}
