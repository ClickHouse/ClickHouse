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
    String trimDatabaseName(const std::string & loadable_definition_name, const String & database_name)
    {
        if (!startsWith(loadable_definition_name, database_name))
            throw Exception(
                "Loadable '" + loadable_definition_name + "' is not from database '" + database_name, ErrorCodes::UNKNOWN_DICTIONARY);
        ///    dbname.loadable_name
        ///--> remove <---
        return loadable_definition_name.substr(database_name.length() + 1);
    }
}


ExternalLoaderDatabaseConfigRepository::ExternalLoaderDatabaseConfigRepository(IDatabase & database_)
    : database_name(database_.getDatabaseName())
    , database(database_)
{
}

LoadablesConfigurationPtr ExternalLoaderDatabaseConfigRepository::load(const std::string & loadable_definition_name)
{
    return database.getDictionaryConfiguration(trimDatabaseName(loadable_definition_name, database_name));
}

bool ExternalLoaderDatabaseConfigRepository::exists(const std::string & loadable_definition_name)
{
    return database.isDictionaryExist(trimDatabaseName(loadable_definition_name, database_name));
}

Poco::Timestamp ExternalLoaderDatabaseConfigRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    return database.getObjectMetadataModificationTime(trimDatabaseName(loadable_definition_name, database_name));
}

std::set<std::string> ExternalLoaderDatabaseConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> result;
    auto itr = database.getDictionariesIterator();
    while (itr && itr->isValid())
    {
        result.insert(database_name + "." + itr->name());
        itr->next();
    }
    return result;
}

}
