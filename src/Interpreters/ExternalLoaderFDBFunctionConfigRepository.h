#pragma once

#include <base/types.h>
#include <unordered_set>
#include <mutex>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>
#include <Databases/IDatabase.h>
#include <Interpreters/FunctionsMetaStoreFDB.h>

namespace DB
{
class Context;
class FunctionsMetaStoreFDB;
/// FDB config repository used by ExternalLoader.
/// Represents config in FDB.
class ExternalLoaderFDBFunctionConfigRepository : public IExternalLoaderConfigRepository, WithContext
{
public:
    explicit ExternalLoaderFDBFunctionConfigRepository(std::string func_name_, ContextPtr context_);
    ~ExternalLoaderFDBFunctionConfigRepository() override = default;
    std::string getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    /// Checks that Function with name exists on fdb
    bool exists(const std::string & definition_entity_name) override;

    /// Return modification time via stat call
    Poco::Timestamp getUpdateTime(const std::string & definition_entity_name) override;

    /// Load functions's metadata from FDB
    LoadablesConfigurationPtr load(const std::string & func_name) override;

    void setFDBFunctionRepositories(std::shared_ptr<FunctionsMetaStoreFDB> FDB_Function_repositories_)
    {
        fdb_function_repositories = FDB_Function_repositories_;
    }

    std::string getFuncName() { return func_name; }

private:
    std::string name;
    std::string func_name;
    std::shared_ptr<FunctionsMetaStoreFDB> fdb_function_repositories;
};

}
