#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>

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
    String dictname = trimDatabaseName(loadable_definition_name, database);
    return getDictionaryConfigurationFromAST(database.getCreateDictionaryQuery(context, dictname)->as<const ASTCreateQuery &>());
}

bool ExternalLoaderDatabaseConfigRepository::exists(const std::string & loadable_definition_name)
{
    return database.isDictionaryExist(context, trimDatabaseName(loadable_definition_name, database));
}

Poco::Timestamp ExternalLoaderDatabaseConfigRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    return database.getObjectMetadataModificationTime(trimDatabaseName(loadable_definition_name, database));
}

std::set<std::string> ExternalLoaderDatabaseConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> result;
    const auto & dbname = database.getDatabaseName();
    auto itr = database.getDictionariesIterator(context);
    while (itr && itr->isValid())
    {
        result.insert(dbname + "." + itr->name());
        itr->next();
    }
    return result;
}

}
