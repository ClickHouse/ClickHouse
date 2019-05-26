#pragma once

#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>
#include <Databases/IDatabase.h>

#include <tuple>
#include <unordered_map>


/// General functionality for several different database engines.

namespace DB
{

class Context;

using DictionariesMap = std::unordered_map<std::string, DictionaryPtr>;


/// Copies list of dictionaries and iterates trough such snapshot.
class DatabaseSnapshotDictionariesIterator final : public IDatabaseIterator
{
private:
    DictionariesMap dictionaries;
    DictionariesMap::iterator it;
    mutable StoragePtr storage_ptr = {};

public:
    explicit DatabaseSnapshotDictionariesIterator(const DictionariesMap & dictionaries_)
        : dictionaries(dictionaries_), it(dictionaries.begin()) {}

    explicit DatabaseSnapshotDictionariesIterator(DictionariesMap && dictionaries_)
        : dictionaries(std::move(dictionaries_)), it(dictionaries.begin()) {}

    void next() override { ++it; }

    bool isValid() const override { return it != dictionaries.end(); }

    const String & name() const override { return it->first; }

    DictionaryPtr & dictionary() const override { return it->second; }

    StoragePtr & table() const override { return storage_ptr; }
};


/** Get the row with the table definition based on the CREATE query.
  * It is an ATTACH query that you can execute to create a table from the correspondent database.
  * See the implementation.
  */
String getTableDefinitionFromCreateQuery(const ASTPtr & query);


/** Create a table by its definition, without using InterpreterCreateQuery.
  *  (InterpreterCreateQuery has more complex functionality, and it can not be used if the database has not been created yet)
  * Returns the table name and the table itself.
  * You must subsequently call IStorage::startup method to use the table.
  */
std::pair<String, StoragePtr> createTableFromDefinition(
    const String & definition,
    const String & database_name,
    const String & database_data_path,
    Context & context,
    bool has_force_restore_data_flag,
    const String & description_for_error_message);


String getDictionaryDefinitionFromCreateQuery(const ASTPtr & query);

std::pair<String, DictionaryPtr> createDictionaryFromDefinition(
    const String & definition,
    const String & database_name,
    Context & context,
    const String & description_for_error_message);

/// Copies list of tables and iterates through such snapshot.
class DatabaseSnapshotIterator final : public IDatabaseIterator
{
private:
    Tables tables;
    Tables::iterator it;
    mutable DictionaryPtr dict_ptr = {};

public:
    DatabaseSnapshotIterator(Tables & tables_)
        : tables(tables_), it(tables.begin()) {}

    DatabaseSnapshotIterator(Tables && tables_)
        : tables(tables_), it(tables.begin()) {}

    void next() override { ++it; }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->first; }

    StoragePtr & table() const override { return it->second; }

    DictionaryPtr & dictionary() const override { return dict_ptr; }
};

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public IDatabase
{
public:
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

    void createDictionary(
        Context & context,
        const String & dictionary_name,
        const DictionaryPtr & dict_ptr,
        const ASTPtr & create) override;

    void removeDictionary(Context & context, const String & dictionary_name) override;

    void loadDictionaries(
        Context & context,
        ThreadPool * thread_pool) override;

    DatabaseIteratorPtr getDictionaryIterator(const Context&) override
    {
        return std::make_unique<DatabaseSnapshotDictionariesIterator>(DictionariesMap());
    }

    bool empty(const Context & context) const override;

    void attachTable(const String & table_name, const StoragePtr & table) override;

    StoragePtr detachTable(const String & table_name) override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    void shutdown() override;

    virtual ~DatabaseWithOwnTablesBase() override;

protected:
    String name;

    mutable std::mutex tables_mutex;
    Tables tables;

    DatabaseWithOwnTablesBase(String name_) : name(std::move(name_)) { }
};

}
