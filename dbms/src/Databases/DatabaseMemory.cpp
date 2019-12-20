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

DatabaseMemory::DatabaseMemory(String name_)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , log(&Logger::get("DatabaseMemory(" + name + ")"))
{}

void DatabaseMemory::loadStoredObjects(
    Context & /*context*/,
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


void DatabaseMemory::attachDictionary(const String & /*name*/, const Context & /*context*/)
{
    throw Exception("There is no ATTACH DICTIONARY query for DatabaseMemory", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseMemory::createDictionary(
    const Context & /*context*/,
    const String & /*dictionary_name*/,
    const ASTPtr & /*query*/)
{
    throw Exception("There is no CREATE DICTIONARY query for DatabaseMemory", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseMemory::removeTable(
    const Context & /*context*/,
    const String & table_name)
{
    detachTable(table_name);
}


void DatabaseMemory::detachDictionary(const String & /*name*/, const Context & /*context*/)
{
    throw Exception("There is no DETACH DICTIONARY query for DatabaseMemory", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseMemory::removeDictionary(
    const Context & /*context*/,
    const String & /*dictionary_name*/)
{
    throw Exception("There is no DROP DICTIONARY query for DatabaseMemory", ErrorCodes::UNSUPPORTED_METHOD);
}


time_t DatabaseMemory::getObjectMetadataModificationTime(
    const Context &, const String &)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseMemory::getCreateTableQuery(
    const Context &,
    const String &) const
{
    throw Exception("There is no CREATE TABLE query for DatabaseMemory tables", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}


ASTPtr DatabaseMemory::getCreateDictionaryQuery(
    const Context &,
    const String &) const
{
    throw Exception("There is no CREATE DICTIONARY query for DatabaseMemory dictionaries", ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY);
}


ASTPtr DatabaseMemory::getCreateDatabaseQuery(
    const Context &) const
{
    throw Exception("There is no CREATE DATABASE query for DatabaseMemory", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
}

String DatabaseMemory::getDatabaseName() const
{
    return name;
}

}
