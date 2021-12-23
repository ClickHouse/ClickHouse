#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#   include <mysqlxx/PoolFactory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(ContextPtr global_context_)
    : ExternalLoader("external dictionary", &Poco::Logger::get("ExternalDictionariesLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"dictionary", "name", "database", "uuid"});
    enableAsyncLoading(true);
    enablePeriodicUpdates(true);
}

ExternalLoader::LoadablePtr ExternalDictionariesLoader::create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config, const std::string & repository_name) const
{
    /// For dictionaries from databases (created with DDL queries) we have to perform
    /// additional checks, so we identify them here.
    bool created_from_ddl = !repository_name.empty();
    return DictionaryFactory::instance().create(name, config, key_in_config, getContext(), created_from_ddl);
}

ExternalDictionariesLoader::DictPtr ExternalDictionariesLoader::getDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    return std::static_pointer_cast<const IDictionary>(load(resolved_dictionary_name));
}

ExternalDictionariesLoader::DictPtr ExternalDictionariesLoader::tryGetDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    return std::static_pointer_cast<const IDictionary>(tryLoad(resolved_dictionary_name));
}


void ExternalDictionariesLoader::reloadDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    loadOrReload(resolved_dictionary_name);
}

DictionaryStructure ExternalDictionariesLoader::getDictionaryStructure(const std::string & dictionary_name, ContextPtr query_context) const
{
    std::string resolved_name = resolveDictionaryName(dictionary_name, query_context->getCurrentDatabase());

    auto load_result = getLoadResult(resolved_name);

    if (load_result.object)
    {
        const auto dictionary = std::static_pointer_cast<const IDictionary>(load_result.object);
        return dictionary->getStructure();
    }

    if (!load_result.config)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary {} config not found", backQuote(dictionary_name));

    return ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
}

QualifiedTableName ExternalDictionariesLoader::qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, ContextPtr query_context) const
{
    auto qualified_name = QualifiedTableName::tryParseFromString(dictionary_name);
    if (!qualified_name)
    {
        QualifiedTableName qualified_dictionary_name;
        qualified_dictionary_name.table = dictionary_name;
        return qualified_dictionary_name;
    }

    /// If dictionary was not qualified with database name, try to resolve dictionary as xml dictionary.
    if (qualified_name->database.empty() && !has(qualified_name->table))
    {
        std::string current_database_name = query_context->getCurrentDatabase();
        std::string resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name, current_database_name);

        /// If after qualify dictionary_name with default_database_name we find it, add default_database to qualified name.
        if (has(resolved_name))
            qualified_name->database = std::move(current_database_name);
    }

    return *qualified_name;
}

std::string ExternalDictionariesLoader::resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const
{
    if (has(dictionary_name))
        return dictionary_name;

    std::string resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name, current_database_name);

    if (has(resolved_name))
        return resolved_name;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary ({}) not found", backQuote(dictionary_name));
}

std::string ExternalDictionariesLoader::resolveDictionaryNameFromDatabaseCatalog(const std::string & name, const std::string & current_database_name) const
{
    /// If it's dictionary from Atomic database, then we need to convert qualified name to UUID.
    /// Try to split name and get id from associated StorageDictionary.
    /// If something went wrong, return name as is.

    String res = name;

    auto qualified_name = QualifiedTableName::tryParseFromString(name);
    if (!qualified_name)
        return res;

    if (qualified_name->database.empty())
    {
        /// Ether database name is not specified and we should use current one
        /// or it's an XML dictionary.
        bool is_xml_dictionary = has(name);
        if (is_xml_dictionary)
            return res;

        qualified_name->database = current_database_name;
        res = current_database_name + '.' + name;
    }

    auto [db, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(
        {qualified_name->database, qualified_name->table},
        const_pointer_cast<Context>(getContext()));

    if (!db)
        return res;
    assert(table);

    if (db->getUUID() == UUIDHelpers::Nil)
        return res;
    if (table->getName() != "Dictionary")
        return res;

    return toString(table->getStorageID().uuid);
}

DictionaryStructure
ExternalDictionariesLoader::getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config)
{
    return DictionaryStructure(config, key_in_config);
}

DictionaryStructure ExternalDictionariesLoader::getDictionaryStructure(const ObjectConfig & config)
{
    return getDictionaryStructure(*config.config, config.key_in_config);
}


void ExternalDictionariesLoader::resetAll()
{
#if USE_MYSQL
    mysqlxx::PoolFactory::instance().reset();
#endif
}

}
