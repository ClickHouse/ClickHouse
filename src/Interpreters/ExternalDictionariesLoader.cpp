#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

#include "config.h"

#if USE_MYSQL
#   include <mysqlxx/PoolFactory.h>
#endif


namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(ContextPtr global_context_)
    : ExternalLoader("external dictionary", getLogger("ExternalDictionariesLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"dictionary", "name", "database", "uuid"});
    enableAsyncLoading(true);
    if (getContext()->getApplicationType() == Context::ApplicationType::SERVER)
        enablePeriodicUpdates(true);
}

ExternalLoader::LoadableMutablePtr ExternalDictionariesLoader::createObject(
        const std::string & name, const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config, const std::string & repository_name) const
{
    /// For dictionaries from databases (created with DDL queries) we have to perform
    /// additional checks, so we identify them here.
    bool created_from_ddl = !repository_name.empty();
    return DictionaryFactory::instance().create(name, config, key_in_config, getContext(), created_from_ddl);
}

bool ExternalDictionariesLoader::doesConfigChangeRequiresReloadingObject(const Poco::Util::AbstractConfiguration & old_config, const String & old_key_in_config,
                                                                         const Poco::Util::AbstractConfiguration & new_config, const String & new_key_in_config) const
{
    std::unordered_set<std::string_view> ignore_keys;
    ignore_keys.insert("comment"); /// We always can change the comment without reloading a dictionary.

    /// If the database is atomic then a dictionary can be renamed without reloading.
    if (!old_config.getString(old_key_in_config + ".uuid", "").empty() && !new_config.getString(new_key_in_config + ".uuid", "").empty())
    {
        ignore_keys.insert("name");
        ignore_keys.insert("database");
    }

    return !isSameConfiguration(old_config, old_key_in_config, new_config, new_key_in_config, ignore_keys);
}

void ExternalDictionariesLoader::updateObjectFromConfigWithoutReloading(IExternalLoadable & object, const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const
{
    IDictionary & dict = static_cast<IDictionary &>(object);

    auto new_dictionary_id = StorageID::fromDictionaryConfig(config, key_in_config);
    auto old_dictionary_id = dict.getDictionaryID();
    if ((new_dictionary_id.table_name != old_dictionary_id.table_name) || (new_dictionary_id.database_name != old_dictionary_id.database_name))
    {
        /// We can update the dictionary ID without reloading only if it's in the atomic database.
        if ((new_dictionary_id.uuid == old_dictionary_id.uuid) && (new_dictionary_id.uuid != UUIDHelpers::Nil))
            dict.updateDictionaryID(new_dictionary_id);
    }

    dict.updateDictionaryComment(config.getString(key_in_config + ".comment", ""));
}

ExternalDictionariesLoader::DictPtr ExternalDictionariesLoader::getDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());

    /// Check if we have a cancellable query context
    QueryStatusPtr process_list_element;
    if (local_context->hasQueryContext())
        process_list_element = local_context->getProcessListElement();

    std::shared_ptr<const IDictionary> dictionary;
    if (process_list_element)
    {
        /// Wait with periodic cancellation checks
        while (true)
        {
            auto result = tryLoad<LoadResult>(resolved_dictionary_name, /* timeout = */ std::chrono::milliseconds(1000));
            if (result.object)
            {
                dictionary = std::static_pointer_cast<const IDictionary>(std::move(result.object));
                break;
            }
            /// If loading has terminally failed, call load() to throw the proper error
            if (result.status != Status::LOADING && result.status != Status::NOT_LOADED)
            {
                dictionary = std::static_pointer_cast<const IDictionary>(load(resolved_dictionary_name));
                break;
            }
            /// Check if the query was cancelled while we were waiting
            process_list_element->checkTimeLimit();
        }
    }
    else
    {
        dictionary = std::static_pointer_cast<const IDictionary>(load(resolved_dictionary_name));
    }

    if (local_context->hasQueryContext() && local_context->getSettingsRef()[Setting::log_queries])
        local_context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Dictionary, dictionary->getQualifiedName());

    return dictionary;
}

ExternalDictionariesLoader::DictPtr ExternalDictionariesLoader::tryGetDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    auto dictionary = std::static_pointer_cast<const IDictionary>(tryLoad(resolved_dictionary_name));

    if (local_context->hasQueryContext() && local_context->getSettingsRef()[Setting::log_queries] && dictionary)
        local_context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Dictionary, dictionary->getQualifiedName());

    return dictionary;
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

void ExternalDictionariesLoader::assertDictionaryStructureExists(const std::string & dictionary_name, ContextPtr query_context) const
{
    getDictionaryStructure(dictionary_name, query_context);
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
        /// Either database name is not specified and we should use current one
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


void ExternalDictionariesLoader::reloadAllTriedToLoadInOrder() const
{
    auto all_names = getAllTriedToLoadNames();
    if (all_names.empty())
        return;

    /// Create a filter matching all previously tried-to-load names
    std::unordered_set<String> names_set(all_names.begin(), all_names.end());

    /// Get load results with configs to extract dependency information
    auto load_results = getLoadResults<LoadResults>(
        [&names_set](const String & name) { return names_set.contains(name); });

    /// Build mapping: QualifiedTableName -> loader_name
    /// This allows resolving source table references back to loader names.
    std::unordered_map<QualifiedTableName, String> qualified_name_to_loader;

    struct DictInfo
    {
        String loader_name;
        String database;
        DictionaryConfigurationPtr config;
    };

    std::vector<DictInfo> dict_infos;
    dict_infos.reserve(load_results.size());

    for (const auto & result : load_results)
    {
        if (!result.config)
            continue;

        auto storage_id = StorageID::fromDictionaryConfig(*result.config->config, result.config->key_in_config);
        QualifiedTableName qname{storage_id.database_name, storage_id.table_name};
        qualified_name_to_loader[qname] = result.name;

        dict_infos.push_back({result.name, storage_id.database_name, result.config->config});
    }

    /// Build dependency graph using reverse adjacency list for Kahn's algorithm.
    /// dependents[A] contains all dictionaries that depend on A (i.e., source from A).
    std::unordered_map<String, Strings> dependents;
    std::unordered_map<String, size_t> in_degree;

    for (const auto & name : all_names)
        in_degree[name] = 0;

    for (const auto & info : dict_infos)
    {
        DictionaryConfigurationPtr config_copy = info.config;
        std::optional<ClickHouseDictionarySourceInfo> source_info;
        try
        {
            source_info = getInfoIfClickHouseDictionarySource(config_copy, getContext());
        }
        catch (...)
        {
            LOG_WARNING(getLogger("ExternalDictionariesLoader"),
                "Failed to parse source config for dictionary '{}', skipping dependency resolution: {}",
                info.loader_name, getCurrentExceptionMessage(false));
            continue;
        }

        if (!source_info || !source_info->is_local || source_info->table_name.table.empty())
            continue;

        /// Resolve source table to a loader name.
        /// If source database is empty, try the dictionary's own database first.
        String dep_name;
        if (source_info->table_name.database.empty() && !info.database.empty())
        {
            QualifiedTableName with_own_db{info.database, source_info->table_name.table};
            auto it = qualified_name_to_loader.find(with_own_db);
            if (it != qualified_name_to_loader.end())
                dep_name = it->second;
        }

        /// Try with the source database as-is (either explicitly specified or empty for XML dicts)
        if (dep_name.empty())
        {
            auto it = qualified_name_to_loader.find(source_info->table_name);
            if (it != qualified_name_to_loader.end())
                dep_name = it->second;
        }

        if (!dep_name.empty() && dep_name != info.loader_name)
        {
            dependents[dep_name].push_back(info.loader_name);
            in_degree[info.loader_name]++;
        }
    }

    /// Kahn's algorithm: produce levels of dictionaries with equal depth
    std::vector<Strings> levels;

    Strings current_level;
    for (const auto & name : all_names)
    {
        if (in_degree[name] == 0)
            current_level.push_back(name);
    }

    std::unordered_set<String> processed;
    while (!current_level.empty())
    {
        levels.push_back(current_level);
        for (const auto & name : current_level)
            processed.insert(name);

        Strings next_level;
        for (const auto & name : current_level)
        {
            auto it = dependents.find(name);
            if (it == dependents.end())
                continue;
            for (const auto & dep : it->second)
            {
                if (--in_degree[dep] == 0)
                    next_level.push_back(dep);
            }
        }

        current_level = std::move(next_level);
    }

    /// Handle circular dependencies: add remaining unprocessed nodes to a final level
    Strings circular;
    for (const auto & name : all_names)
    {
        if (!processed.contains(name))
            circular.push_back(name);
    }

    if (!circular.empty())
    {
        auto logger = getLogger("ExternalDictionariesLoader");
        for (const auto & name : circular)
            LOG_WARNING(logger, "Dictionary '{}' is part of a circular dependency chain, will be reloaded last", name);
        levels.push_back(std::move(circular));
    }

    /// Reload level by level. Errors at one level don't block subsequent levels.
    std::exception_ptr first_exception;
    for (const auto & level : levels)
    {
        std::unordered_set<String> level_set(level.begin(), level.end());
        auto level_filter = [&level_set](const String & name) { return level_set.contains(name); };

        try
        {
            loadOrReload<LoadResults>(level_filter);
        }
        catch (...)
        {
            if (!first_exception)
                first_exception = std::current_exception();
        }
    }

    if (first_exception)
        std::rethrow_exception(first_exception);
}


void ExternalDictionariesLoader::resetAll()
{
#if USE_MYSQL
    mysqlxx::PoolFactory::instance().reset();
#endif
}

}
