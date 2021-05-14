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
    bool dictionary_from_database = !repository_name.empty();
    return DictionaryFactory::instance().create(name, config, key_in_config, getContext(), dictionary_from_database);
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
    if (!load_result.config)
        throw Exception("Dictionary " + backQuote(dictionary_name) + " config not found", ErrorCodes::BAD_ARGUMENTS);

    return ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
}

std::string ExternalDictionariesLoader::resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const
{
    std::string resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name);
    bool has_dictionary = has(resolved_name);

    if (!has_dictionary)
    {
        /// If dictionary not found. And database was not implicitly specified
        /// we can qualify dictionary name with current database name.
        /// It will help if dictionary is created with DDL and is in current database.
        if (dictionary_name.find('.') == std::string::npos)
        {
            String dictionary_name_with_database = current_database_name + '.' + dictionary_name;
            resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name_with_database);
            has_dictionary = has(resolved_name);
        }
    }

    if (!has_dictionary)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary ({}) not found", backQuote(dictionary_name));

    return resolved_name;
}

std::string ExternalDictionariesLoader::resolveDictionaryNameFromDatabaseCatalog(const std::string & name) const
{
    /// If it's dictionary from Atomic database, then we need to convert qualified name to UUID.
    /// Try to split name and get id from associated StorageDictionary.
    /// If something went wrong, return name as is.

    auto pos = name.find('.');
    if (pos == std::string::npos || name.find('.', pos + 1) != std::string::npos)
        return name;

    std::string maybe_database_name = name.substr(0, pos);
    std::string maybe_table_name = name.substr(pos + 1);

    auto [db, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable({maybe_database_name, maybe_table_name}, getContext());
    if (!db)
        return name;
    assert(table);

    if (db->getUUID() == UUIDHelpers::Nil)
        return name;
    if (table->getName() != "Dictionary")
        return name;

    return toString(table->getStorageID().uuid);
}

DictionaryStructure
ExternalDictionariesLoader::getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config)
{
    return {config, key_in_config};
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
