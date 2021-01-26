#pragma once

#include <common/types.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Dictionaries/IDictionary.h>
#include <Databases/DictionaryAttachInfo.h>
#include <Common/Exception.h>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <ctime>
#include <functional>
#include <memory>


namespace DB
{

class Context;
struct Settings;
struct ConstraintsDescription;
struct IndicesDescription;
class ASTCreateQuery;
using DictionariesWithID = std::vector<std::pair<String, UUID>>;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
}

class IDatabaseTablesIterator
{
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;

    virtual const String & name() const = 0;

    /// This method can return nullptr if it's Lazy database
    /// (a database with support for lazy tables loading
    /// - it maintains a list of tables but tables are loaded lazily).
    virtual const StoragePtr & table() const = 0;

    virtual ~IDatabaseTablesIterator() = default;

    virtual UUID uuid() const { return UUIDHelpers::Nil; }

    const String & databaseName() const { assert(!database_name.empty()); return database_name; }

protected:
    String database_name;
};

/// Copies list of tables and iterates through such snapshot.
class DatabaseTablesSnapshotIterator : public IDatabaseTablesIterator
{
private:
    Tables tables;
    Tables::iterator it;

protected:
    DatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && other)
    {
        size_t idx = std::distance(other.tables.begin(), other.it);
        std::swap(tables, other.tables);
        other.it = other.tables.end();
        it = tables.begin();
        std::advance(it, idx);
        database_name = std::move(other.database_name);
    }

public:
    DatabaseTablesSnapshotIterator(const Tables & tables_, const String & database_name_)
    : tables(tables_), it(tables.begin())
    {
        database_name = database_name_;
    }

    DatabaseTablesSnapshotIterator(Tables && tables_, String && database_name_)
    : tables(std::move(tables_)), it(tables.begin())
    {
        database_name = std::move(database_name_);
    }

    void next() override { ++it; }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->first; }

    const StoragePtr & table() const override { return it->second; }
};

/// Copies list of dictionaries and iterates through such snapshot.
class DatabaseDictionariesSnapshotIterator
{
private:
    DictionariesWithID dictionaries;
    DictionariesWithID::iterator it;
    String database_name;

public:
    DatabaseDictionariesSnapshotIterator() = default;
    DatabaseDictionariesSnapshotIterator(DictionariesWithID & dictionaries_, const String & database_name_)
    : dictionaries(dictionaries_), it(dictionaries.begin()), database_name(database_name_)
    {
    }
    DatabaseDictionariesSnapshotIterator(DictionariesWithID && dictionaries_, const String & database_name_)
    : dictionaries(dictionaries_), it(dictionaries.begin()), database_name(database_name_)
    {
    }

    void next() { ++it; }

    bool isValid() const { return !dictionaries.empty() && it != dictionaries.end(); }

    const String & name() const { return it->first; }

    const UUID & uuid() const { return it->second; }

    const String & databaseName() const { assert(!database_name.empty()); return database_name; }
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

    virtual bool canContainMergeTreeTables() const { return true; }

    virtual bool canContainDistributedTables() const { return true; }

    /// Load a set of existing tables.
    /// You can call only once, right after the object is created.
    virtual void loadStoredObjects(Context & /*context*/, bool /*has_force_restore_data_flag*/, bool /*force_attach*/ = false) {}

    /// Check the existence of the table.
    virtual bool isTableExist(const String & name, const Context & context) const = 0;

    /// Check the existence of the dictionary
    virtual bool isDictionaryExist(const String & /*name*/) const
    {
        return false;
    }

    /// Get the table for work. Return nullptr if there is no table.
    virtual StoragePtr tryGetTable(const String & name, const Context & context) const = 0;

    virtual UUID tryGetTableUUID(const String & /*table_name*/) const { return UUIDHelpers::Nil; }

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Get an iterator that allows you to pass through all the tables.
    /// It is possible to have "hidden" tables that are not visible when passing through, but are visible if you get them by name using the functions above.
    virtual DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) = 0;

    /// Get an iterator to pass through all the dictionaries.
    virtual DatabaseDictionariesIteratorPtr getDictionariesIterator([[maybe_unused]] const FilterByNameFunction & filter_by_dictionary_name = {})
    {
        return std::make_unique<DatabaseDictionariesSnapshotIterator>();
    }

    /// Is the database empty.
    virtual bool empty() const = 0;

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

    /// Delete the table from the database, drop table and delete the metadata.
    virtual void dropTable(
        const Context & /*context*/,
        const String & /*name*/,
        [[maybe_unused]] bool no_delay = false)
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
    virtual void attachTable(const String & /*name*/, const StoragePtr & /*table*/, [[maybe_unused]] const String & relative_table_path = {})
    {
        throw Exception("There is no ATTACH TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Add dictionary to the database, but do not add it to the metadata. The database may not support this method.
    /// If dictionaries_lazy_load is false it also starts loading the dictionary asynchronously.
    virtual void attachDictionary(const String & /* dictionary_name */, const DictionaryAttachInfo & /* attach_info */)
    {
        throw Exception("There is no ATTACH DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Forget about the table without deleting it, and return it. The database may not support this method.
    virtual StoragePtr detachTable(const String & /*name*/)
    {
        throw Exception("There is no DETACH TABLE query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Forget about the dictionary without deleting it. The database may not support this method.
    virtual void detachDictionary(const String & /*name*/)
    {
        throw Exception("There is no DETACH DICTIONARY query for Database" + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Rename the table and possibly move the table to another database.
    virtual void renameTable(
        const Context & /*context*/,
        const String & /*name*/,
        IDatabase & /*to_database*/,
        const String & /*to_name*/,
        bool /*exchange*/,
        bool /*dictionary*/)
    {
        throw Exception(getEngineName() + ": renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    using ASTModifier = std::function<void(IAST &)>;

    /// Change the table structure in metadata.
    /// You must call under the alter_lock of the corresponding table . If engine_modifier is empty, then engine does not change.
    virtual void alterTable(
        const Context & /*context*/,
        const StorageID & /*table_id*/,
        const StorageInMemoryMetadata & /*metadata*/)
    {
        throw Exception(getEngineName() + ": alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Returns time of table's metadata change, 0 if there is no corresponding metadata file.
    virtual time_t getObjectMetadataModificationTime(const String & /*name*/) const
    {
        return static_cast<time_t>(0);
    }

    /// Get the CREATE TABLE query for the table. It can also provide information for detached tables for which there is metadata.
    ASTPtr tryGetCreateTableQuery(const String & name, const Context & context) const noexcept
    {
        return getCreateTableQueryImpl(name, context, false);
    }

    ASTPtr getCreateTableQuery(const String & name, const Context & context) const
    {
        return getCreateTableQueryImpl(name, context, true);
    }

    /// Get the CREATE DICTIONARY query for the dictionary. Returns nullptr if dictionary doesn't exists.
    ASTPtr tryGetCreateDictionaryQuery(const String & name) const noexcept
    {
        return getCreateDictionaryQueryImpl(name, false);
    }

    ASTPtr getCreateDictionaryQuery(const String & name) const
    {
        return getCreateDictionaryQueryImpl(name, true);
    }

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> getDictionaryConfiguration(const String & /*name*/) const
    {
        throw Exception(getEngineName() + ": getDictionaryConfiguration() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Get the CREATE DATABASE query for current database.
    virtual ASTPtr getCreateDatabaseQuery() const = 0;

    /// Get name of database.
    String getDatabaseName() const
    {
        std::lock_guard lock{mutex};
        return database_name;
    }
    /// Get UUID of database.
    virtual UUID getUUID() const { return UUIDHelpers::Nil; }

    virtual void renameDatabase(const String & /*new_name*/)
    {
        throw Exception(getEngineName() + ": RENAME DATABASE is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Returns path for persistent data storage if the database supports it, empty string otherwise
    virtual String getDataPath() const { return {}; }

    /// Returns path for persistent data storage for table if the database supports it, empty string otherwise. Table must exist
    virtual String getTableDataPath(const String & /*table_name*/) const { return {}; }
    /// Returns path for persistent data storage for CREATE/ATTACH query if the database supports it, empty string otherwise
    virtual String getTableDataPath(const ASTCreateQuery & /*query*/) const { return {}; }
    /// Returns metadata path if the database supports it, empty string otherwise
    virtual String getMetadataPath() const { return {}; }
    /// Returns metadata path of a concrete table if the database supports it, empty string otherwise
    virtual String getObjectMetadataPath(const String & /*table_name*/) const { return {}; }

    /// All tables and dictionaries should be detached before detaching the database.
    virtual bool shouldBeEmptyOnDetach() const { return true; }

    /// Ask all tables to complete the background threads they are using and delete all table objects.
    virtual void shutdown() = 0;

    /// Delete data and metadata stored inside the database, if exists.
    virtual void drop(const Context & /*context*/) {}

    virtual ~IDatabase() {}

protected:
    virtual ASTPtr getCreateTableQueryImpl(const String & /*name*/, const Context & /*context*/, bool throw_on_error) const
    {
        if (throw_on_error)
            throw Exception("There is no SHOW CREATE TABLE query for Database" + getEngineName(), ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
        return nullptr;
    }

    virtual ASTPtr getCreateDictionaryQueryImpl(const String & /*name*/, bool throw_on_error) const
    {
        if (throw_on_error)
            throw Exception("There is no SHOW CREATE DICTIONARY query for Database" + getEngineName(), ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY);
        return nullptr;
    }

    mutable std::mutex mutex;
    String database_name;
};

using DatabasePtr = std::shared_ptr<IDatabase>;
using Databases = std::map<String, DatabasePtr>;

}
