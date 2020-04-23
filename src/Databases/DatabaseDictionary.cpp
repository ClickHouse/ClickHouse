#include <Databases/DatabaseDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryStructure.h>
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
    extern const int SYNTAX_ERROR;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
}

namespace
{
    StoragePtr createStorageDictionary(const String & database_name, const ExternalLoader::LoadResult & load_result)
    {
        if (!load_result.config)
            return nullptr;
        DictionaryStructure dictionary_structure = ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
        return StorageDictionary::create(StorageID(database_name, load_result.name), load_result.name, dictionary_structure);
    }
}

DatabaseDictionary::DatabaseDictionary(const String & name_)
    : IDatabase(name_),
      log(&Logger::get("DatabaseDictionary(" + database_name + ")"))
{
}

Tables DatabaseDictionary::listTables(const Context & context, const FilterByNameFunction & filter_by_name)
{
    Tables tables;
    auto load_results = context.getExternalDictionariesLoader().getLoadResults(filter_by_name);
    for (auto & load_result : load_results)
    {
        auto storage = createStorageDictionary(getDatabaseName(), load_result);
        if (storage)
            tables.emplace(storage->getStorageID().table_name, storage);
    }
    return tables;
}

bool DatabaseDictionary::isTableExist(
    const Context & context,
    const String & table_name) const
{
    return context.getExternalDictionariesLoader().getCurrentStatus(table_name) != ExternalLoader::Status::NOT_EXIST;
}

StoragePtr DatabaseDictionary::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    auto load_result = context.getExternalDictionariesLoader().getLoadResult(table_name);
    return createStorageDictionary(getDatabaseName(), load_result);
}

DatabaseTablesIteratorPtr DatabaseDictionary::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(listTables(context, filter_by_table_name));
}

bool DatabaseDictionary::empty(const Context & context) const
{
    return !context.getExternalDictionariesLoader().hasObjects();
}

ASTPtr DatabaseDictionary::getCreateTableQueryImpl(const Context & context,
                                                   const String & table_name, bool throw_on_error) const
{
    String query;
    {
        WriteBufferFromString buffer(query);

        auto load_result = context.getExternalDictionariesLoader().getLoadResult(table_name);
        if (!load_result.config)
        {
            if (throw_on_error)
                throw Exception{"Dictionary " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY};
            return {};
        }

        auto names_and_types = StorageDictionary::getNamesAndTypes(ExternalDictionariesLoader::getDictionaryStructure(*load_result.config));
        buffer << "CREATE TABLE " << backQuoteIfNeed(database_name) << '.' << backQuoteIfNeed(table_name) << " (";
        buffer << StorageDictionary::generateNamesAndTypesDescription(names_and_types);
        buffer << ") Engine = Dictionary(" << backQuoteIfNeed(table_name) << ")";
    }

    auto settings = context.getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message,
            /* hilite = */ false, "", /* allow_multi_statements = */ false, 0, settings.max_parser_depth);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr DatabaseDictionary::getCreateDatabaseQuery(const Context & context) const
{
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(database_name) << " ENGINE = Dictionary";
    }
    auto settings = context.getSettingsRef();
    ParserCreateQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
}

void DatabaseDictionary::shutdown()
{
}

}
