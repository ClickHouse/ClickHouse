#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Databases/IDatabase.h>

namespace DB
{

class StorageDictionary;

class ExternalLoaderDictionaryStorageConfigRepository : public IExternalLoaderConfigRepository
{
public:
    explicit ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_storage_);

    std::string getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

private:
    const StorageDictionary & dictionary_storage;
};

}
