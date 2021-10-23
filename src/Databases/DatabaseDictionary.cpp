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
    StoragePtr createStorageDictionary(const String & database_name, const ExternalLoader::LoadResult & load_result, ContextPtr context)
    {
        try
        {
            if (!load_result.config)
                return nullptr;

            DictionaryStructure dictionary_structure = ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
            return StorageDictionary::create(
                StorageID(database_name, load_result.name),
                load_result.name,
                dictionary_structure,
                StorageDictionary::Location::DictionaryDatabase,
                context);
        }
        catch (Exception & e)
        {
            throw Exception(
                fmt::format("Error while loading dictionary '{}.{}': {}",
                    database_name, load_result.name, e.displayText()),
                e.code());
        }
    }
}

DatabaseDictionary::DatabaseDictionary(const String & name_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("DatabaseDictionary(" + database_name + ")"))
{
}

Tables DatabaseDictionary::listTables(const FilterByNameFunction & filter_by_name)
{
    Tables tables;
    auto load_results = getContext()->getExternalDictionariesLoader().getLoadResults(filter_by_name);
    String db_name = getDatabaseName();
    for (auto & load_result : load_results)
    {
        auto storage = createStorageDictionary(db_name, load_result, getContext());
        if (storage)
            tables.emplace(storage->getStorageID().table_name, storage);
    }
    return tables;
}

bool DatabaseDictionary::isTableExist(const String & table_name, ContextPtr) const
{
    return getContext()->getExternalDictionariesLoader().getCurrentStatus(table_name) != ExternalLoader::Status::NOT_EXIST;
}

StoragePtr DatabaseDictionary::tryGetTable(const String & table_name, ContextPtr) const
{
    auto load_result = getContext()->getExternalDictionariesLoader().getLoadResult(table_name);
    return createStorageDictionary(getDatabaseName(), load_result, getContext());
}

DatabaseTablesIteratorPtr DatabaseDictionary::getTablesIterator(ContextPtr, const FilterByNameFunction & filter_by_table_name)
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(listTables(filter_by_table_name), getDatabaseName());
}

bool DatabaseDictionary::empty() const
{
    return !getContext()->getExternalDictionariesLoader().hasObjects();
}

ASTPtr DatabaseDictionary::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    String query;
    {
        WriteBufferFromString buffer(query);

        auto load_result = getContext()->getExternalDictionariesLoader().getLoadResult(table_name);
        if (!load_result.config)
        {
            if (throw_on_error)
                throw Exception{"Dictionary " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY};
            return {};
        }

        auto names_and_types = StorageDictionary::getNamesAndTypes(ExternalDictionariesLoader::getDictionaryStructure(*load_result.config));
        buffer << "CREATE TABLE " << backQuoteIfNeed(getDatabaseName()) << '.' << backQuoteIfNeed(table_name) << " (";
        buffer << StorageDictionary::generateNamesAndTypesDescription(names_and_types);
        buffer << ") Engine = Dictionary(" << backQuoteIfNeed(table_name) << ")";
    }

    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message,
            /* hilite = */ false, "", /* allow_multi_statements = */ false, 0, settings.max_parser_depth);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr DatabaseDictionary::getCreateDatabaseQuery() const
{
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(getDatabaseName()) << " ENGINE = Dictionary";
    }
    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
}

void DatabaseDictionary::shutdown()
{
}

}
