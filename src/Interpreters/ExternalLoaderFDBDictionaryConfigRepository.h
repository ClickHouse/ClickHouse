#pragma once

#include <base/types.h>
#include <unordered_set>
#include <mutex>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class Context;
struct StorageID;
class DictionariesMetaStoreFDB;

/// FDB config repository used by ExternalLoader.
/// Represents config in FDB.
class ExternalLoaderFDBDictionaryConfigRepository : public IExternalLoaderConfigRepository, WithContext
{
public:
    explicit ExternalLoaderFDBDictionaryConfigRepository(std::string dict_name_, ContextPtr context_);
    std::string getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    /// Checks that dictionary with name exists on fdb
    bool exists(const std::string & definition_entity_name) override;

    /// Return modification time via stat call
    Poco::Timestamp getUpdateTime(const std::string & definition_entity_name) override;

    /// Load dictionary metadata from fdb.
    LoadablesConfigurationPtr load(const std::string & dict_name) override;

    void setFDBDictionaryRepositories(std::shared_ptr<DictionariesMetaStoreFDB> FDB_dictionary_repositories_)
    {
        fdb_dictionary_repositories = FDB_dictionary_repositories_;
    }
    std::string getDictName() { return dict_name; }

private:
    std::string name; /// Repository name
    std::string dict_name;
    std::shared_ptr<DictionariesMetaStoreFDB> fdb_dictionary_repositories;
};

}
