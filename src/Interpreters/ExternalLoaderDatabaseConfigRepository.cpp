#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DICTIONARY;
}


namespace
{
    String trimDatabaseName(const std::string & loadable_definition_name, const IDatabase & database)
    {
        const auto & dbname = database.getDatabaseName();
        if (!startsWith(loadable_definition_name, dbname))
            throw Exception(
                "Loadable '" + loadable_definition_name + "' is not from database '" + database.getDatabaseName(), ErrorCodes::UNKNOWN_DICTIONARY);
        ///    dbname.loadable_name
        ///--> remove <---
        return loadable_definition_name.substr(dbname.length() + 1);
    }
}


ExternalLoaderDatabaseConfigRepository::ExternalLoaderDatabaseConfigRepository(IDatabase & database_, const Context & context_)
    : name(database_.getDatabaseName())
    , database(database_)
    , context(context_)
{
}

LoadablesConfigurationPtr ExternalLoaderDatabaseConfigRepository::load(const std::string & loadable_definition_name)
{
    return database.getDictionaryConfiguration(trimDatabaseName(loadable_definition_name, database));
}

bool ExternalLoaderDatabaseConfigRepository::exists(const std::string & loadable_definition_name)
{
    return database.isDictionaryExist(trimDatabaseName(loadable_definition_name, database));
}

Poco::Timestamp ExternalLoaderDatabaseConfigRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    return database.getObjectMetadataModificationTime(trimDatabaseName(loadable_definition_name, database));
}

std::set<std::string> ExternalLoaderDatabaseConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> result;
    const auto & dbname = database.getDatabaseName();
    auto itr = database.getDictionariesIterator();
    while (itr && itr->isValid())
    {
        result.insert(dbname + "." + itr->name());
        itr->next();
    }
    return result;
}

}
