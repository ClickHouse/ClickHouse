#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageDictionary.h>


namespace DB
{

class StorageDictionary;

class ExternalLoaderDictionaryStorageConfigRepository : public IExternalLoaderConfigRepository
{
public:
    ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_);

    const std::string & getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

private:
    const StorageDictionary & dictionary;
    const String internal_dictionary_name;
};

}
