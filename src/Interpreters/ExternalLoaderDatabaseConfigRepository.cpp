#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DICTIONARY;
}


namespace
{
    String trimDatabaseName(const std::string & loadable_definition_name, const String & database_name,
                            const IDatabase & database, const Context & global_context)
    {
        bool is_atomic_database = database.getUUID() != UUIDHelpers::Nil;
        if (is_atomic_database)
        {
            /// We do not know actual database and dictionary names here
            auto dict_id = StorageID::createEmpty();
            dict_id.uuid = parseFromString<UUID>(loadable_definition_name);
            assert(dict_id.uuid != UUIDHelpers::Nil);
            /// Get associated StorageDictionary by UUID
            auto table = DatabaseCatalog::instance().getTable(dict_id, global_context);
            auto dict_id_with_names = table->getStorageID();
            return dict_id_with_names.table_name;
        }

        if (!startsWith(loadable_definition_name, database_name))
            throw Exception(ErrorCodes::UNKNOWN_DICTIONARY,
                            "Loadable '{}' is not from database '{}'", loadable_definition_name, database_name);
        ///    dbname.loadable_name
        ///--> remove <---
        return loadable_definition_name.substr(database_name.length() + 1);
    }
}


ExternalLoaderDatabaseConfigRepository::ExternalLoaderDatabaseConfigRepository(IDatabase & database_, const Context & context_)
    : global_context(context_.getGlobalContext())
    , database_name(database_.getDatabaseName())
    , database(database_)
{
}

LoadablesConfigurationPtr ExternalLoaderDatabaseConfigRepository::load(const std::string & loadable_definition_name)
{
    auto dict_name = trimDatabaseName(loadable_definition_name, database_name, database, global_context);
    return database.getDictionaryConfiguration(dict_name);
}

bool ExternalLoaderDatabaseConfigRepository::exists(const std::string & loadable_definition_name)
{
    auto dict_name = trimDatabaseName(loadable_definition_name, database_name, database, global_context);
    return database.isDictionaryExist(dict_name);
}

Poco::Timestamp ExternalLoaderDatabaseConfigRepository::getUpdateTime(const std::string & loadable_definition_name)
{
    auto dict_name = trimDatabaseName(loadable_definition_name, database_name, database, global_context);
    return database.getObjectMetadataModificationTime(dict_name);
}

std::set<std::string> ExternalLoaderDatabaseConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> result;
    auto itr = database.getDictionariesIterator();
    bool is_atomic_database = database.getUUID() != UUIDHelpers::Nil;
    while (itr && itr->isValid())
    {
        if (is_atomic_database)
        {
            assert(itr->uuid() != UUIDHelpers::Nil);
            result.insert(toString(itr->uuid()));
        }
        else
            result.insert(database_name + "." + itr->name());
        itr->next();
    }
    return result;
}

}
