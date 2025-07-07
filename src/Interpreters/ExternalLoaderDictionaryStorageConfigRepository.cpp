#include "ExternalLoaderDictionaryStorageConfigRepository.h"

#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>

namespace DB
{

ExternalLoaderDictionaryStorageConfigRepository::ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_storage_)
    : dictionary_storage(dictionary_storage_)
{
}

std::string ExternalLoaderDictionaryStorageConfigRepository::getName() const
{
    return dictionary_storage.getStorageID().getInternalDictionaryName();
}

std::set<std::string> ExternalLoaderDictionaryStorageConfigRepository::getAllLoadablesDefinitionNames()
{
    return { getName() };
}

bool ExternalLoaderDictionaryStorageConfigRepository::exists(const std::string & loadable_definition_name)
{
    return getName() == loadable_definition_name;
}

LoadablesConfigurationPtr ExternalLoaderDictionaryStorageConfigRepository::load(const std::string &)
{
    return dictionary_storage.getConfiguration();
}

}
