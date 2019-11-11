#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Poco/File.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
    data_path = "store/";
}

String DatabaseAtomic::getDataPath(const String & table_name) const
{
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception("Table " + table_name + " not found in database " + getDatabaseName(), ErrorCodes::UNKNOWN_TABLE);
    return data_path + it->second;
}

String DatabaseAtomic::getDataPath(const ASTCreateQuery & query) const
{
    stringToUUID(query.uuid);   /// Check UUID is valid
    const size_t uuid_prefix_len = 3;
    return data_path + query.uuid.substr(0, uuid_prefix_len) + '/' + query.uuid + '/';
}

void DatabaseAtomic::drop(const Context &)
{
    Poco::File(getMetadataPath()).remove(false);
}

void DatabaseAtomic::createTable(const Context & context, const String & table_name, const StoragePtr & table,
                                 const ASTPtr & query)
{
    String relative_table_path = getDataPath(query->as<ASTCreateQuery &>());
    DatabaseOnDisk::createTable(context, table_name, table, query);

}

void DatabaseAtomic::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    DatabaseWithDictionaries::attachTable(name, table, relative_table_path);
    std::lock_guard lock(mutex);
    table_name_to_path.emplace(std::make_pair(table->getTableName(), relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    {
        std::lock_guard lock(mutex);
        table_name_to_path.erase(name);
    }
    return DatabaseWithDictionaries::detachTable(name);
}


}

