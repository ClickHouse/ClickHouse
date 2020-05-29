#pragma once

#include "config_core.h"
#if USE_MYSQL

#    include <mutex>
#    include <Core/MySQLClient.h>
#    include <DataStreams/BlockIO.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/IDatabase.h>
#    include <Databases/MySQL/MasterStatusInfo.h>
#    include <Interpreters/MySQL/CreateQueryVisitor.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <mysqlxx/Pool.h>
#    include <mysqlxx/PoolWithFailover.h>

namespace DB
{

class DatabaseMaterializeMySQL : public IDatabase
{
public:
    DatabaseMaterializeMySQL(
        const Context & context, const String & database_name_, const String & metadata_path_,
        const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_,
        MySQLClient && client_);

    String getEngineName() const override { return "MySQL"; }

    void shutdown() override { nested_database->shutdown(); }

    bool empty() const override { return nested_database->empty(); }

    String getDataPath() const override { return nested_database->getDataPath(); }

    String getTableDataPath(const String & string) const override { return nested_database->getTableDataPath(string); }

    String getTableDataPath(const ASTCreateQuery & query) const override { return nested_database->getTableDataPath(query); }

    UUID tryGetTableUUID(const String & string) const override { return nested_database->tryGetTableUUID(string); }

    bool isDictionaryExist(const String & string) const override { return nested_database->isDictionaryExist(string); }

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const FilterByNameFunction & filter_by_dictionary_name) override
    {
        return nested_database->getDictionariesIterator(filter_by_dictionary_name);
    }

    void createTable(const Context & context, const String & string, const StoragePtr & ptr, const ASTPtr & astPtr) override
    {
        nested_database->createTable(context, string, ptr, astPtr);
    }

    void createDictionary(const Context & context, const String & string, const ASTPtr & ptr) override
    {
        nested_database->createDictionary(context, string, ptr);
    }

    void dropTable(const Context & context, const String & string, bool no_delay) override
    {
        nested_database->dropTable(context, string, no_delay);
    }

    void removeDictionary(const Context & context, const String & string) override { nested_database->removeDictionary(context, string); }

    void attachTable(const String & string, const StoragePtr & ptr, const String & relative_table_path) override
    {
        nested_database->attachTable(string, ptr, relative_table_path);
    }

    void attachDictionary(const String & string, const DictionaryAttachInfo & info) override { nested_database->attachDictionary(string, info); }

    StoragePtr detachTable(const String & string) override { return nested_database->detachTable(string); }

    void detachDictionary(const String & string) override { nested_database->detachDictionary(string); }

    void renameTable(const Context & context, const String & string, IDatabase & database, const String & string1, bool b) override
    {
        nested_database->renameTable(context, string, database, string1, b);
    }

    void alterTable(const Context & context, const StorageID & id, const StorageInMemoryMetadata & metadata) override
    {
        nested_database->alterTable(context, id, metadata);
    }

    time_t getObjectMetadataModificationTime(const String & string) const override
    {
        return nested_database->getObjectMetadataModificationTime(string);
    }

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> getDictionaryConfiguration(const String & string) const override
    {
        return nested_database->getDictionaryConfiguration(string);
    }

    String getMetadataPath() const override { return nested_database->getMetadataPath(); }

    String getObjectMetadataPath(const String & string) const override { return nested_database->getObjectMetadataPath(string); }

    bool shouldBeEmptyOnDetach() const override { return nested_database->shouldBeEmptyOnDetach(); }

    void drop(const Context & context) override { nested_database->drop(context); }

    bool isTableExist(const String & name) const override { return nested_database->isTableExist(name); }

    StoragePtr tryGetTable(const String & name) const override { return nested_database->tryGetTable(name); }

    void loadStoredObjects(Context & context, bool b) override
    {
        try
        {
            LOG_DEBUG(log, "Loading MySQL nested database stored objects.");
            nested_database->loadStoredObjects(context, b);
            LOG_DEBUG(log, "Loaded MySQL nested database stored objects.");
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot load MySQL nested database stored objects.");
            throw;
        }
    }

    ASTPtr getCreateDatabaseQuery() const override
    {
        const auto & create_query = std::make_shared<ASTCreateQuery>();
        create_query->database = database_name;
        create_query->set(create_query->storage, database_engine_define);
        return create_query;
    }

    DatabaseTablesIteratorPtr getTablesIterator(const FilterByNameFunction & filter_by_table_name) override { return nested_database->getTablesIterator(filter_by_table_name); }

private:
    const Context & global_context;
    String metadata_path;
    ASTPtr database_engine_define;
    String mysql_database_name;
    DatabasePtr nested_database;

    size_t version{0};
    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;

    Poco::Logger * log;

    void synchronized();

    MasterStatusInfo prepareSynchronized(std::unique_lock<std::mutex> & lock);

    BlockOutputStreamPtr getTableOutput(const String & table_name, bool fill_version);

    BlockIO tryToExecuteQuery(const String & query_to_execute, const String & comment);

    String getCreateQuery(const mysqlxx::Pool::Entry & connection, const String & table_name);

    void dumpDataForTables(mysqlxx::Pool::Entry & connection, const std::vector<String> & tables_name, const std::function<bool()> & is_cancelled);

    mutable std::mutex sync_mutex;
    std::atomic<bool> sync_quit{false};
    std::condition_variable sync_cond;
    ThreadFromGlobalPool thread{&DatabaseMaterializeMySQL::synchronized, this};
};

}

#endif
