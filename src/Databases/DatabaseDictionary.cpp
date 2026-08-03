#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Storages/StorageDictionary.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>
#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

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
            auto comment = load_result.config->config->getString("dictionary.comment", "");

            return std::make_shared<StorageDictionary>(
                StorageID(database_name, load_result.name),
                load_result.name,
                dictionary_structure,
                comment,
                StorageDictionary::Location::DictionaryDatabase,
                context);
        }
        catch (Exception & e)
        {
            throw Exception(e.code(),
                "Error while loading dictionary '{}.{}': {}",
                    database_name, load_result.name, e.displayText());
        }
    }
}

DatabaseDictionary::DatabaseDictionary(const String & name_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext())
    , log(getLogger("DatabaseDictionary(" + database_name + ")"))
{
}

Tables DatabaseDictionary::listTables(const FilterByNameFunction & filter_by_name) const
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

DatabaseTablesIteratorPtr DatabaseDictionary::getTablesIterator(ContextPtr, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
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
                throw Exception(ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY, "Dictionary {} doesn't exist", backQuote(table_name));
            return {};
        }

        auto names_and_types = StorageDictionary::getNamesAndTypes(ExternalDictionariesLoader::getDictionaryStructure(*load_result.config), false);
        buffer << "CREATE TABLE " << backQuoteIfNeed(getDatabaseName()) << '.' << backQuoteIfNeed(table_name) << " (";
        buffer << names_and_types.toNamesAndTypesDescription();
        buffer << ") Engine = Dictionary(" << backQuoteIfNeed(table_name) << ")";
    }

    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "",
        /* allow_multi_statements = */ false,
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks],
        true);

    if (!ast && throw_on_error)
        throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr DatabaseDictionary::getCreateDatabaseQuery() const
{
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(getDatabaseName()) << " ENGINE = Dictionary";
        if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
            buffer << " COMMENT " << backQuote(comment_value);
    }
    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;
    return parseQuery(
        parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
}

void DatabaseDictionary::shutdown()
{
}

void registerDatabaseDictionary(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        return make_shared<DatabaseDictionary>(
            args.database_name,
            args.context);
    };
    factory.registerDatabase("Dictionary", create_fn);
}
}
