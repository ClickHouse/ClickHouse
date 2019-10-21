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
    ExternalLoaderDatabaseConfigRepository(const DatabasePtr & database_, const Context & context_)
        : database(database_)
        , context(context_)
    {
    }

    std::set<std::string> getAllLoadablesDefinitionNames() const override;

    bool exists(const std::string & loadable_definition_name) const override;

    Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) const override;

private:
    DatabasePtr database;
    Context context;
};

}
