#pragma once

#include <mutex>
#include <unordered_set>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>

#include <Poco/Path.h>

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
public:
    DatabaseDictionary(const String & name_, const Poco::Path & metadata_path_, const Context & context_);

    String getDatabaseName() const override;

    String getEngineName() const override
    {
        return "Dictionary";
    }

    void loadTables(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) override;

    void loadDictionaries(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) override;

    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    bool isDictionaryExist(
        const Context & context,
        const String & dictionary_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) const override;

    DictionaryPtr tryGetDictionary(
        const Context & context,
        const String & dictionary_name) const override;

    DictionaryPtr getDictionary(
        const Context & context,
        const String & dictionary_name) const override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    DatabaseIteratorPtr getDictionaryIterator(const Context & context) override;

    bool empty(const Context & context) const override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void createDictionary(
        Context & context,
        const String & dictionary_name,
        const DictionaryPtr & dict_ptr,
        const ASTPtr & query) override;

    void attachDictionary(
        const String & name,
        DictionaryPtr dictionary);

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void removeDictionary(
        Context & context,
        const String & dictionary_name) override;

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
        const ColumnsDescription & columns,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(
            const Context & context,
            const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    String getMetadataPath() const override;

    void shutdown() override;

private:
    const String name;
    Poco::Path metadata_path;
    mutable std::mutex mutex;
    const ExternalDictionaries & external_dictionaries;
    std::unordered_set<String> deleted_tables;

    Poco::Logger * log;

    Tables loadTables();

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};

}
