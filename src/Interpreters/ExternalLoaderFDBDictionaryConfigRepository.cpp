#include <Interpreters/ExternalLoaderFDBDictionaryConfigRepository.h>
#include <vector>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/DictionariesMetaStoreFDB.h>

DB::ExternalLoaderFDBDictionaryConfigRepository::ExternalLoaderFDBDictionaryConfigRepository(std::string dict_name_, ContextPtr context_)
    : WithContext(context_), name(dict_name_), dict_name(dict_name_), fdb_dictionary_repositories(getContext()->getFDBDictionaryLoader())
{
}

std::string DB::ExternalLoaderFDBDictionaryConfigRepository::getName() const
{
    return dict_name;
}

std::set<std::string> DB::ExternalLoaderFDBDictionaryConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> re;
    re.insert(dict_name);
    return re;
}

/// Checks that dictionary with name exists on fdb
bool DB::ExternalLoaderFDBDictionaryConfigRepository::exists(const std::string & definition_entity_name)
{
    return fdb_dictionary_repositories->exist(definition_entity_name);
}

/// Return modification time via stat call
Poco::Timestamp DB::ExternalLoaderFDBDictionaryConfigRepository::getUpdateTime(const std::string & definition_entity_name)
{
    return fdb_dictionary_repositories->getUpdateTime(definition_entity_name);
}

/// Load dictionary metadata from fdb by dictionary name.
DB::LoadablesConfigurationPtr DB::ExternalLoaderFDBDictionaryConfigRepository::load(const std::string & dict_name)
{
    return fdb_dictionary_repositories->getOneDictConfig(dict_name);
}
