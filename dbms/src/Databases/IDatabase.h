#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>
#include <ctime>
#include <memory>
#include <functional>
#include <Poco/File.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>


class ThreadPool;


namespace DB
{

class Context;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

struct Settings;


/** Allows to iterate over tables.
  */
class IDatabaseIterator
{
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;

    virtual const String & name() const = 0;
    virtual StoragePtr & table() const = 0;

    virtual ~IDatabaseIterator() {}
};

using DatabaseIteratorPtr = std::unique_ptr<IDatabaseIterator>;


/** Database engine.
  * It is responsible for:
  * - initialization of set of known tables;
  * - checking existence of a table and getting a table object;
  * - retrieving a list of all tables;
  * - creating and dropping tables;
  * - renaming tables and moving between databases with same engine.
  */

class IDatabase : public std::enable_shared_from_this<IDatabase>
{
public:
    /// Get name of database engine.
    virtual String getEngineName() const = 0;

    /// Load a set of existing tables. If thread_pool is specified, use it.
    /// You can call only once, right after the object is created.
    virtual void loadTables(
        Context & context,
        ThreadPool * thread_pool,
        bool has_force_restore_data_flag) = 0;

    /// Check the existence of the table.
    virtual bool isTableExist(
        const Context & context,
        const String & name) const = 0;

    /// Get the table for work. Return nullptr if there is no table.
    virtual StoragePtr tryGetTable(
        const Context & context,
        const String & name) const = 0;

    /// Get an iterator that allows you to pass through all the tables.
    /// It is possible to have "hidden" tables that are not visible when passing through, but are visible if you get them by name using the functions above.
    virtual DatabaseIteratorPtr getIterator(const Context & context) = 0;

    /// Is the database empty.
    virtual bool empty(const Context & context) const = 0;

    /// Add the table to the database. Record its presence in the metadata.
    virtual void createTable(
        const Context & context,
        const String & name,
        const StoragePtr & table,
        const ASTPtr & query) = 0;

    /// Delete the table from the database and return it. Delete the metadata.
    virtual void removeTable(
        const Context & context,
        const String & name) = 0;

    /// Add a table to the database, but do not add it to the metadata. The database may not support this method.
    virtual void attachTable(const String & name, const StoragePtr & table) = 0;

    /// Forget about the table without deleting it, and return it. The database may not support this method.
    virtual StoragePtr detachTable(const String & name) = 0;

    /// Rename the table and possibly move the table to another database.
    virtual void renameTable(
        const Context & context,
        const String & name,
        IDatabase & to_database,
        const String & to_name) = 0;

    using ASTModifier = std::function<void(IAST &)>;

    /// Change the table structure in metadata.
    /// You must call under the TableStructureLock of the corresponding table . If engine_modifier is empty, then engine does not change.
    virtual void alterTable(
        const Context & context,
        const String & name,
        const ColumnsDescription & columns,
        const ASTModifier & engine_modifier) = 0;

    /// Returns time of table's metadata change, 0 if there is no corresponding metadata file.
    virtual time_t getTableMetadataModificationTime(
        const Context & context,
        const String & name) = 0;

    /// Get the CREATE TABLE query for the table. It can also provide information for detached tables for which there is metadata.
    virtual ASTPtr tryGetCreateTableQuery(const Context & context, const String & name) const = 0;

    virtual ASTPtr getCreateTableQuery(const Context & context, const String & name) const
    {
        return tryGetCreateTableQuery(context, name);
    }

    /// Get the CREATE DATABASE query for current database.
    virtual ASTPtr getCreateDatabaseQuery(const Context & context) const = 0;

    /// Get name of database.
    virtual String getDatabaseName() const = 0;
    /// Returns path for persistent data storage if the database supports it, empty string otherwise
    virtual String getDataPath() const { return {}; }
    /// Returns metadata path if the database supports it, empty string otherwise
    virtual String getMetadataPath() const { return {}; }
    /// Returns metadata path of a concrete table if the database supports it, empty string otherwise
    virtual String getTableMetadataPath(const String & /*table_name*/) const { return {}; }

    /// Ask all tables to complete the background threads they are using and delete all table objects.
    virtual void shutdown() = 0;

    /// Delete data and metadata stored inside the database, if exists.
    virtual void drop() {}

    virtual ~IDatabase() {}
};

using DatabasePtr = std::shared_ptr<IDatabase>;
using Databases = std::map<String, DatabasePtr>;

}

