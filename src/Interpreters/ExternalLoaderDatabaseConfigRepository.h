#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Databases/IDatabase.h>


namespace DB
{

/// Repository from database, which stores dictionary definitions on disk.
/// Tracks update time and existence of .sql files through IDatabase.
class ExternalLoaderDatabaseConfigRepository : public IExternalLoaderConfigRepository, WithContext
{
public:
    ExternalLoaderDatabaseConfigRepository(IDatabase & database_, ContextPtr global_context_);

    std::string getName() const override { return database_name; }

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

private:
    const String database_name;
    IDatabase & database;
};

}
