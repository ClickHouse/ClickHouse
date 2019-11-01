#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Dictionaries/IDictionary.h>
#include <Common/Exception.h>

#include <ctime>
#include <functional>
#include <memory>


namespace DB
{

class Context;
struct Settings;
struct ConstraintsDescription;
class ColumnsDescription;
struct IndicesDescription;
struct TableStructureWriteLockHolder;
using Dictionaries = std::set<String>;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
}

class IDatabaseTablesIterator
{
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;

    virtual const String & name() const = 0;
    virtual const StoragePtr & table() const = 0;

    virtual ~IDatabaseTablesIterator() = default;
};

/// Copies list of tables and iterates through such snapshot.
class DatabaseTablesSnapshotIterator : public IDatabaseTablesIterator
{
private:
    Tables tables;
    Tables::iterator it;

public:
    DatabaseTablesSnapshotIterator(Tables & tables_) : tables(tables_), it(tables.begin()) {}

    DatabaseTablesSnapshotIterator(Tables && tables_) : tables(tables_), it(tables.begin()) {}

    void next() { ++it; }

    bool isValid() const { return it != tables.end(); }

    const String & name() const { return it->first; }

    const StoragePtr & table() const { return it->second; }
};

/// Copies list of dictionaries and iterates through such snapshot.
class DatabaseDictionariesSnapshotIterator
{
private:
    Dictionaries dictionaries;
    Dictionaries::iterator it;

public:
    DatabaseDictionariesSnapshotIterator() = default;
    DatabaseDictionariesSnapshotIterator(Dictionaries & dictionaries_) : dictionaries(dictionaries_), it(dictionaries.begin()) {}

    DatabaseDictionariesSnapshotIterator(Dictionaries && dictionaries_) : dictionaries(dictionaries_), it(dictionaries.begin()) {}

    void next() { ++it; }

    bool isValid() const { return !dictionaries.empty() && it != dictionaries.end(); }

    const String & name() const { return *it; }
};

using DatabaseTablesIteratorPtr = std::unique_ptr<IDatabaseTablesIterator>;
using DatabaseDictionariesIteratorPtr = std::unique_ptr<DatabaseDictionariesSnapshotIterator>;


/** Database engine.
  * It is responsible for:
  * - initialization of set of known tables and dictionaries;
  * - checking existence of a table and getting a table object;
  * - retrieving a list of all tables;
  * - creating and dropping tables;
  * - renaming tables and moving between databases with same engine.
  */

class IDatabase : public std::enable_shared_from_this<IDatabase>
{
public:
    IDatabase() = delete;
    IDatabase(String database_name_) : database_name(std::move(database_name_)) {}

    /// Get name of database engine.
    virtual String getEngineName() const = 0;

    /// Load a set of existing tables.
    /// You can call only once, right after the object is created.
    virtual void loadStoredObjects(Context & /*context*/, bool /*has_force_restore_data_flag*/) {}

    /// Check the existence of the table.
    virtual bool isTableExist(
        const Context & context,
        const String & name) const = 0;

    /// Check the existence of the dictionary
    virtual bool isDictionaryExist(
        const Context & /*context*/,
        const String & /*name*/) const
    {
        return false;
    }

    /// Get the table for work. Return nullptr if there is no table.
    virtual StoragePtr tryGetTable(
        const Context & context,
        const String & name) const = 0;

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Get an iterator that allows you to pass through all the tables.
    /// It is possible to have "hidden" tables that are not visible when passing through, but are visible if you get them by name using the functions above.
    virtual DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) = 0;

    /// Get an iterator to pass through all the dictionaries.
    virtual DatabaseDictionariesIteratorPtr getDictionariesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) = 0;

    /// Get an iterator to pass through all the tables and dictionary tables.
    virtual DatabaseTablesIteratorPtr getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_name = {})
    {
        return getTablesIterator(context, filter_by_name);
    }

    /// Is the database empty.
    virtual bool empty(const Context & context) const = 0;

    /// Add the table to the database. Record its presence in the metadata.
    virtual void createTable(
        const Context & /*context*/,
        const String & /*name*/,
        const StoragePtr & /*table*/,
        const ASTPtr & /*query*/)
    {
        throw Exception("There is no CREATE TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Add the dictionary to the database. Record its presence in the metadata.
    virtual void createDictionary(
        const Context & /*context*/,
        const String & /*dictionary_name*/,
        const ASTPtr & /*query*/)
    {
        throw Exception("There is no CREATE DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Delete the table from the database. Delete the metadata.
    virtual void removeTable(
        const Context & /*context*/,
        const String & /*name*/)
    {
        throw Exception("There is no DROP TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Delete the dictionary from the database. Delete the metadata.
    virtual void removeDictionary(
        const Context & /*context*/,
        const String & /*dictionary_name*/)
    {
        throw Exception("There is no DROP DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Add a table to the database, but do not add it to the metadata. The database may not support this method.
    virtual void attachTable(const String & /*name*/, const StoragePtr & /*table*/)
    {
        throw Exception("There is no ATTACH TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Add dictionary to the database, but do not add it to the metadata. The database may not support this method.
    /// If dictionaries_lazy_load is false it also starts loading the dictionary asynchronously.
    virtual void attachDictionary(const String & /*name*/, const Context & /*context*/)
    {
        throw Exception("There is no ATTACH DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Forget about the table without deleting it, and return it. The database may not support this method.
    virtual StoragePtr detachTable(const String & /*name*/)
    {
        throw Exception("There is no DETACH TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Forget about the dictionary without deleting it. The database may not support this method.
    virtual void detachDictionary(const String & /*name*/, const Context & /*context*/)
    {
        throw Exception("There is no DETACH DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Rename the table and possibly move the table to another database.
    virtual void renameTable(
        const Context & /*context*/,
        const String & /*name*/,
        IDatabase & /*to_database*/,
        const String & /*to_name*/,
        TableStructureWriteLockHolder &)
    {
        throw Exception(getEngineName() + ": renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    using ASTModifier = std::function<void(IAST &)>;

    /// Change the table structure in metadata.
    /// You must call under the TableStructureLock of the corresponding table . If engine_modifier is empty, then engine does not change.
    virtual void alterTable(
        const Context & /*context*/,
        const String & /*name*/,
        const ColumnsDescription & /*columns*/,
        const IndicesDescription & /*indices*/,
        const ConstraintsDescription & /*constraints*/,
        const ASTModifier & /*engine_modifier*/)
    {
        throw Exception(getEngineName() + ": renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Returns time of table's metadata change, 0 if there is no corresponding metadata file.
    virtual time_t getObjectMetadataModificationTime(const Context & /*context*/, const String & /*name*/)
    {
        return static_cast<time_t>(0);
    }

    /// Get the CREATE TABLE query for the table. It can also provide information for detached tables for which there is metadata.
    ASTPtr tryGetCreateTableQuery(const Context & context, const String & name) const noexcept
    {
        return getCreateTableQueryImpl(context, name, false);
    }

    ASTPtr getCreateTableQuery(const Context & context, const String & name) const
    {
        return getCreateTableQueryImpl(context, name, true);
    }

    /// Get the CREATE DICTIONARY query for the dictionary. Returns nullptr if dictionary doesn't exists.
    ASTPtr tryGetCreateDictionaryQuery(const Context & context, const String & name) const noexcept
    {
        return getCreateDictionaryQueryImpl(context, name, false);
    }

    ASTPtr getCreateDictionaryQuery(const Context & context, const String & name) const
    {
        return getCreateDictionaryQueryImpl(context, name, true);
    }

    /// Get the CREATE DATABASE query for current database.
    virtual ASTPtr getCreateDatabaseQuery(const Context & context) const = 0;

    /// Get name of database.
    String getDatabaseName() const { return database_name; }
    /// Returns path for persistent data storage if the database supports it, empty string otherwise
    virtual String getDataPath() const { return {}; }
    /// Returns metadata path if the database supports it, empty string otherwise
    virtual String getMetadataPath() const { return {}; }
    /// Returns metadata path of a concrete table if the database supports it, empty string otherwise
    virtual String getObjectMetadataPath(const String & /*table_name*/) const { return {}; }

    /// Ask all tables to complete the background threads they are using and delete all table objects.
    virtual void shutdown() = 0;

    /// Delete data and metadata stored inside the database, if exists.
    virtual void drop(const Context & /*context*/) {}

    virtual ~IDatabase() {}

protected:
    virtual ASTPtr getCreateTableQueryImpl(const Context & /*context*/, const String & /*name*/, bool throw_on_error) const
    {
        if (throw_on_error)
            throw Exception("There is no SHOW CREATE TABLE query for Database" + getEngineName(), ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
        return nullptr;
    }

    virtual ASTPtr getCreateDictionaryQueryImpl(const Context & /*context*/, const String & /*name*/, bool throw_on_error) const
    {
        if (throw_on_error)
            throw Exception("There is no SHOW CREATE DICTIONARY query for Database" + getEngineName(), ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY);
        return nullptr;
    }

    String database_name;
};

using DatabasePtr = std::shared_ptr<IDatabase>;
using Databases = std::map<String, DatabasePtr>;

}
