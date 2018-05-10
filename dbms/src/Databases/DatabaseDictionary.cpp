#include <Databases/DatabaseDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Storages/StorageDictionary.h>
#include <common/logger_useful.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}

DatabaseDictionary::DatabaseDictionary(const String & name_, const Context & context)
    : name(name_),
      external_dictionaries(context.getExternalDictionaries()),
      log(&Logger::get("DatabaseDictionary(" + name + ")"))
{
}

void DatabaseDictionary::loadTables(Context &, ThreadPool *, bool)
{
}

Tables DatabaseDictionary::loadTables()
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();

    Tables tables;
    for (const auto & pair : dictionaries)
    {
        const std::string & name = pair.first;
        if (deleted_tables.count(name))
            continue;
        auto dict_ptr = std::static_pointer_cast<IDictionaryBase>(pair.second.loadable);
        if (dict_ptr)
        {
            const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[name] = StorageDictionary::create(name, ColumnsDescription{columns}, dictionary_structure, name);
        }
    }

    return tables;
}

bool DatabaseDictionary::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();
    return dictionaries.count(table_name) && !deleted_tables.count(table_name);
}

StoragePtr DatabaseDictionary::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();

    if (deleted_tables.count(table_name))
        return {};
    {
        auto it = dictionaries.find(table_name);
        if (it != dictionaries.end())
        {
            const auto & dict_ptr = std::static_pointer_cast<IDictionaryBase>(it->second.loadable);
            if (dict_ptr)
            {
                const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
                auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
                return StorageDictionary::create(table_name, ColumnsDescription{columns}, dictionary_structure, table_name);
            }
        }
    }

    return {};
}

DatabaseIteratorPtr DatabaseDictionary::getIterator(const Context & /*context*/)
{
    return std::make_unique<DatabaseSnapshotIterator>(loadTables());
}

bool DatabaseDictionary::empty(const Context & /*context*/) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();
    for (const auto & pair : dictionaries)
        if (pair.second.loadable && !deleted_tables.count(pair.first))
            return false;
    return true;
}

StoragePtr DatabaseDictionary::detachTable(const String & /*table_name*/)
{
    throw Exception("DatabaseDictionary: detachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::attachTable(const String & /*table_name*/, const StoragePtr & /*table*/)
{
    throw Exception("DatabaseDictionary: attachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::createTable(
    const Context & /*context*/,
    const String & /*table_name*/,
    const StoragePtr & /*table*/,
    const ASTPtr & /*query*/)
{
    throw Exception("DatabaseDictionary: createTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::removeTable(
    const Context & context,
    const String & table_name)
{
    if (!isTableExist(context, table_name))
        throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    auto objects_map = external_dictionaries.getObjectsMap();
    deleted_tables.insert(table_name);
}

void DatabaseDictionary::renameTable(
    const Context &,
    const String &,
    IDatabase &,
    const String &)
{
    throw Exception("DatabaseDictionary: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::alterTable(
    const Context &,
    const String &,
    const ColumnsDescription &,
    const ASTModifier &)
{
    throw Exception("DatabaseDictionary: alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseDictionary::getTableMetadataModificationTime(
    const Context &,
    const String &)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseDictionary::getCreateTableQueryImpl(const Context & context,
                                                   const String & table_name, bool throw_on_error) const
{
    String query;
    {
        WriteBufferFromString buffer(query);

        const auto & dictionaries = context.getExternalDictionaries();
        auto dictionary = throw_on_error ? dictionaries.getDictionary(table_name)
                                         : dictionaries.tryGetDictionary(table_name);

        auto names_and_types = StorageDictionary::getNamesAndTypes(dictionary->getStructure());
        buffer << "CREATE TABLE " << backQuoteIfNeed(name) << '.' << backQuoteIfNeed(table_name) << " (";
        buffer << StorageDictionary::generateNamesAndTypesDescription(names_and_types.begin(), names_and_types.end());
        buffer << ") Engine = Dictionary(" << backQuoteIfNeed(table_name) << ")";
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message,
            /* hilite = */ false, "", /* allow_multi_statements = */ false, 0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr DatabaseDictionary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseDictionary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseDictionary::getCreateDatabaseQuery(const Context & /*context*/) const
{
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(name) << " ENGINE = Dictionary";
    }
    ParserCreateQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
}

void DatabaseDictionary::shutdown()
{
}

String DatabaseDictionary::getDatabaseName() const
{
    return name;
}

}
