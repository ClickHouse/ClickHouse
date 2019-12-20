#pragma once

#include <mutex>
#include <unordered_set>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage_fwd.h>


namespace Poco
{
    class Logger;
}


namespace DB
{

/* Database to store StorageDictionary tables
 * automatically creates tables for all dictionaries
 */
class DatabaseDictionary : public IDatabase
{
public:
    DatabaseDictionary(const String & name_);

    String getDatabaseName() const override;

    String getEngineName() const override
    {
        return "Dictionary";
    }

    void loadStoredObjects(
        Context & context,
        bool has_force_restore_data_flag) override;

    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    bool isDictionaryExist(const Context & context, const String & table_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) override;

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) override;

    bool empty(const Context & context) const override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void createDictionary(
        const Context & context, const String & dictionary_name, const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void removeDictionary(const Context & context, const String & table_name) override;

    void attachTable(const String & table_name, const StoragePtr & table) override;

    StoragePtr detachTable(const String & table_name) override;

    time_t getObjectMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(
            const Context & context,
            const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    ASTPtr getCreateDictionaryQuery(const Context & context, const String & table_name) const override;

    ASTPtr tryGetCreateDictionaryQuery(const Context & context, const String & table_name) const override;


    void attachDictionary(const String & dictionary_name, const Context & context) override;

    void detachDictionary(const String & dictionary_name, const Context & context) override;

    void shutdown() override;

private:
    const String name;
    mutable std::mutex mutex;

    Poco::Logger * log;

    Tables listTables(const Context & context, const FilterByNameFunction & filter_by_name);
    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};

}
