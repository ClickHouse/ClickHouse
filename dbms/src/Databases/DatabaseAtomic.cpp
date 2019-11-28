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
    extern const int FILE_DOESNT_EXIST;
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
    data_path = "store/";
    log = &Logger::get("DatabaseAtomic (" + name_ + ")");
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

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, TableStructureWriteLockHolder &)
{
    //FIXME
    if (typeid(*this) != typeid(to_database))
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    table->renameInMemory(to_database.getDatabaseName(), to_table_name);

    ASTPtr ast = getQueryFromMetadata(getObjectMetadataPath(table_name));
    if (!ast)
        throw Exception("There is no metadata file for table " + backQuote(table_name) + ".", ErrorCodes::FILE_DOESNT_EXIST);
    ast->as<ASTCreateQuery &>().table = to_table_name;

    /// NOTE Non-atomic.
    to_database.createTable(context, to_table_name, table, ast);
    removeTable(context, table_name);
}


}

