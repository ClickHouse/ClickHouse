#include <common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
    extern const int UNSUPPORTED_METHOD;
}

DatabaseMemory::DatabaseMemory(const String & name_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")")
{}

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

ASTPtr DatabaseMemory::getCreateDatabaseQuery(
    const Context &) const
{
    //FIXME
    throw Exception("There is no CREATE DATABASE query for DatabaseMemory", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

}
