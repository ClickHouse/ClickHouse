#pragma once

#include <Databases/DatabasesCommon.h>
#include <Dictionaries/IDictionary.h>

#include <Poco/Path.h>

#include <string>
#include <unordered_map>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseWithOwnTablesBase
{
public:
    DatabaseOrdinary(String name_, const String & metadata_path_, const Context & context);

    String getEngineName() const override { return "Ordinary"; }

    void loadTables(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name) override;

    void alterTable(
        const Context & context,
        const String & name,
        const ColumnsDescription & columns,
        const IndicesDescription & indices,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr getCreateDictionaryQuery(
        const Context & context,
        const String & dictionary_name) const override;

    ASTPtr tryGetCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateDictionaryQuery(
        const Context & context,
        const String & dictionary_name) const override;

    void loadDictionaries(
        Context & context,
        ThreadPool * thread_pool) override;

    void createDictionary(
        Context & context,
        const String & dictionary_name,
        const DictionaryPtr & dictionary,
        const ASTPtr & query) override;

    void attachDictionary(
        const String & dictionary_name,
        DictionaryPtr & dictionary);

    bool isDictionaryExist(
        const Context & context,
        const String & dictionary_name) const override;

    DictionaryPtr tryGetDictionary(
        const Context & context,
        const String & dictionary_name) const override;

    DictionaryPtr getDictionary(
        const Context & context,
        const String & dictionary_name) const override;

    void removeDictionary(
        Context & context,
        const String & dictionary_name) override;


    DatabaseIteratorPtr getDictionaryIterator(const Context & context) override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    String getDataPath() const override;
    String getDatabaseName() const override;
    String getMetadataPath() const override;
    String getTableMetadataPath(const String & table_name) const override;

    void drop() override;

    void shutdown() override;

private:
    Poco::Path metadata_path;
    Poco::Path dictionaries_metadata_path;
    const String data_path;
    Poco::Logger * log;

    /// This mutex follow tables_mutex in lock order
    mutable std::mutex dictionaries_mutex;
    DictionariesMap dictionaries;

private:

    void startupTables(ThreadPool * thread_pool);

    ASTPtr getCreateDictionaryQueryImpl(const Context & context, const String & dictionary_name, bool throw_on_error) const;

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;

    std::string getDictionaryMetadataPath(const std::string & dictionary_name) const;
};

}
