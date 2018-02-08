#pragma once

#include <mutex>
#include <unordered_set>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>


namespace Poco
{
    class Logger;
}


namespace DB
{
class ExternalDictionaries;

/* Database to store StorageDictionary tables
 * automatically creates tables for all dictionaries
 */
class DatabaseDictionary : public IDatabase
{
private:
    const String name;
    mutable std::mutex mutex;
    const ExternalDictionaries & external_dictionaries;
    std::unordered_set<String> deleted_tables;

    Poco::Logger * log;

    Tables loadTables();

public:
    DatabaseDictionary(const String & name_, const Context & context);

    String getEngineName() const override
    {
        return "Dictionary";
    }

    void loadTables(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) override;

    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    bool empty(const Context & context) const override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void attachTable(const String & table_name, const StoragePtr & table) override;
    StoragePtr detachTable(const String & table_name) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name) override;

    void alterTable(
        const Context & context,
        const String & name,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateQuery(
        const Context & context,
        const String & table_name) const override;

    String getDataPath(const Context & context) const override;

    void shutdown() override;
    void drop() override;
};

}
