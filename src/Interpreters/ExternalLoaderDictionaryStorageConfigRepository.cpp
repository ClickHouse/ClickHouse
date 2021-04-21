#include "ExternalLoaderDictionaryStorageConfigRepository.h"

#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>

namespace DB
{

ExternalLoaderDictionaryStorageConfigRepository::ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_)
    : dictionary(dictionary_)
    , internal_dictionary_name(dictionary.getStorageID().getInternalDictionaryName())
{
}

const std::string & ExternalLoaderDictionaryStorageConfigRepository::getName() const
{
    return internal_dictionary_name;
}

std::set<std::string> ExternalLoaderDictionaryStorageConfigRepository::getAllLoadablesDefinitionNames()
{
    return { internal_dictionary_name };
}

bool ExternalLoaderDictionaryStorageConfigRepository::exists(const std::string & loadable_definition_name)
{
    return internal_dictionary_name == loadable_definition_name;
}

Poco::Timestamp ExternalLoaderDictionaryStorageConfigRepository::getUpdateTime(const std::string &)
{
    return dictionary.getUpdateTime();
}

LoadablesConfigurationPtr ExternalLoaderDictionaryStorageConfigRepository::load(const std::string &)
{
    return dictionary.getConfiguration();
}

}
