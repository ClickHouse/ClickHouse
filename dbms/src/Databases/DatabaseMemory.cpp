#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}

DatabaseMemory::DatabaseMemory(String name_)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , log(&Logger::get("DatabaseMemory(" + name + ")"))
{}

void DatabaseMemory::loadTables(
    Context & /*context*/,
    ThreadPool * /*thread_pool*/,
    bool /*has_force_restore_data_flag*/)
{
    /// Nothing to load.
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

ASTPtr DatabaseMemory::getCreateTableQuery(
    const Context &,
    const String &) const
{
    throw Exception("There is no CREATE TABLE query for DatabaseMemory tables", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery(
    const Context &) const
{
    throw Exception("There is no CREATE DATABASE query for DatabaseMemory", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

void DatabaseMemory::drop()
{
    /// Additional actions to delete database are not required.
}

}
