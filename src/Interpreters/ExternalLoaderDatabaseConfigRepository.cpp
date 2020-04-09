#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DICTIONARY;
}

namespace
{
String trimDatabaseName(const std::string & loadable_definition_name, const DatabasePtr database)
{
    const auto & dbname = database->getDatabaseName();
    if (!startsWith(loadable_definition_name, dbname))
        throw Exception(
            "Loadable '" + loadable_definition_name + "' is not from database '" + database->getDatabaseName(), ErrorCodes::UNKNOWN_DICTIONARY);
    ///    dbname.loadable_name
    ///--> remove <---
    return loadable_definition_name.substr(dbname.length() + 1);
}
}

LoadablesConfigurationPtr ExternalLoaderDatabaseConfigRepository::load(const std::string & loadable_definition_name) const
{
    String dictname = trimDatabaseName(loadable_definition_name, database);
    return getDictionaryConfigurationFromAST(database->getCreateDictionaryQuery(context, dictname)->as<const ASTCreateQuery &>());
}

bool ExternalLoaderDatabaseConfigRepository::exists(const std::string & loadable_definition_name) const
{
    return database->isDictionaryExist(
                     context, trimDatabaseName(loadable_definition_name, database));
}

Poco::Timestamp ExternalLoaderDatabaseConfigRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    return database->getObjectMetadataModificationTime(context, trimDatabaseName(loadable_definition_name, database));
}

std::set<std::string> ExternalLoaderDatabaseConfigRepository::getAllLoadablesDefinitionNames() const
{
    std::set<std::string> result;
    const auto & dbname = database->getDatabaseName();
    auto itr = database->getDictionariesIterator(context);
    while (itr && itr->isValid())
    {
        result.insert(dbname + "." + itr->name());
        itr->next();
    }
    return result;
}

}
