#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>

namespace DB
{

/// Repository from database, which stores dictionary definitions on disk.
/// Tracks update time and existance of .sql files through IDatabase.
class ExternalLoaderDatabaseConfigRepository : public IExternalLoaderConfigRepository
{
public:
    ExternalLoaderDatabaseConfigRepository(IDatabase & database_, const Context & context_);

    const std::string & getName() const override { return name; }

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

private:
    const String name;
    IDatabase & database;
    Context context;
};

}
