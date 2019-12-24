#include <Databases/DatabaseDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Storages/StorageDictionary.h>
#include <common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

DatabaseDictionary::DatabaseDictionary(const String & name_)
    : name(name_),
      log(&Logger::get("DatabaseDictionary(" + name + ")"))
{
}

void DatabaseDictionary::loadStoredObjects(Context &, bool)
{
}

Tables DatabaseDictionary::listTables(const Context & context, const FilterByNameFunction & filter_by_name)
{
    Tables tables;
    ExternalLoader::LoadResults load_results;
    if (filter_by_name)
    {
        /// If `filter_by_name` is set, we iterate through all dictionaries with such names. That's why we need to load all of them.
        load_results = context.getExternalDictionariesLoader().tryLoad<ExternalLoader::LoadResults>(filter_by_name);
    }
    else
    {
        /// If `filter_by_name` isn't set, we iterate through only already loaded dictionaries. We don't try to load all dictionaries in this case.
        load_results = context.getExternalDictionariesLoader().getCurrentLoadResults();
    }

    for (const auto & load_result: load_results)
    {
        /// Load tables only from XML dictionaries, don't touch other
        if (load_result.object && load_result.repository_name.empty())
        {
            auto dict_ptr = std::static_pointer_cast<const IDictionaryBase>(load_result.object);
            auto dict_name = dict_ptr->getName();
            const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[dict_name] = StorageDictionary::create(getDatabaseName(), dict_name, ColumnsDescription{columns}, context, true, dict_name);
        }
    }
    return tables;
}

bool DatabaseDictionary::isTableExist(
    const Context & context,
    const String & table_name) const
{
    return context.getExternalDictionariesLoader().getCurrentStatus(table_name) != ExternalLoader::Status::NOT_EXIST;
}


bool DatabaseDictionary::isDictionaryExist(
    const Context & /*context*/,
    const String & /*table_name*/) const
{
    return false;
}


DatabaseDictionariesIteratorPtr DatabaseDictionary::getDictionariesIterator(
    const Context & /*context*/,
    const FilterByNameFunction & /*filter_by_dictionary_name*/)
{
    return std::make_unique<DatabaseDictionariesSnapshotIterator>();
}


void DatabaseDictionary::createDictionary(
    const Context & /*context*/,
    const String & /*dictionary_name*/,
    const ASTPtr & /*query*/)
{
    throw Exception("Dictionary engine doesn't support dictionaries.", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseDictionary::removeDictionary(
    const Context & /*context*/,
    const String & /*table_name*/)
{
    throw Exception("Dictionary engine doesn't support dictionaries.", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseDictionary::attachDictionary(
    const String & /*dictionary_name*/, const Context & /*context*/)
{
    throw Exception("Dictionary engine doesn't support dictionaries.", ErrorCodes::UNSUPPORTED_METHOD);
}

void DatabaseDictionary::detachDictionary(const String & /*dictionary_name*/, const Context & /*context*/)
{
    throw Exception("Dictionary engine doesn't support dictionaries.", ErrorCodes::UNSUPPORTED_METHOD);
}


ASTPtr DatabaseDictionary::tryGetCreateDictionaryQuery(
    const Context & /*context*/,
    const String & /*table_name*/) const
{
    return nullptr;
}


ASTPtr DatabaseDictionary::getCreateDictionaryQuery(
    const Context & /*context*/,
    const String & /*table_name*/) const
{
    throw Exception("Dictionary engine doesn't support dictionaries.", ErrorCodes::UNSUPPORTED_METHOD);
}

StoragePtr DatabaseDictionary::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    auto dict_ptr = context.getExternalDictionariesLoader().tryGetDictionary(table_name);
    if (dict_ptr)
    {
        const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
        auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
        return StorageDictionary::create(getDatabaseName(), table_name, ColumnsDescription{columns}, context, true, table_name);
    }

    return {};
}

DatabaseTablesIteratorPtr DatabaseDictionary::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_name)
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(listTables(context, filter_by_name));
}

bool DatabaseDictionary::empty(const Context & context) const
{
    return !context.getExternalDictionariesLoader().hasCurrentlyLoadedObjects();
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
    const Context &,
    const String &,
    const StoragePtr &,
    const ASTPtr &)
{
    throw Exception("DatabaseDictionary: createTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::removeTable(
    const Context &,
    const String &)
{
    throw Exception("DatabaseDictionary: removeTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseDictionary::getObjectMetadataModificationTime(
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

        const auto & dictionaries = context.getExternalDictionariesLoader();
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
