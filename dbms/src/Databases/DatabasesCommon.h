#pragma once

#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>


/// General functionality for several different database engines.

namespace DB
{

class Context;


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


/// Copies list of tables and iterates through such snapshot.
class DatabaseSnapshotIterator : public IDatabaseIterator
{
private:
    Tables tables;
    Tables::iterator it;

public:
    DatabaseSnapshotIterator(Tables & tables_)
        : tables(tables_), it(tables.begin()) {}

    DatabaseSnapshotIterator(Tables && tables_)
        : tables(tables_), it(tables.begin()) {}

    void next() override
    {
        ++it;
    }

    bool isValid() const override
    {
        return it != tables.end();
    }

    const String & name() const override
    {
        return it->first;
    }

    StoragePtr & table() const override
    {
        return it->second;
    }
};

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public IDatabase
{
public:
    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) const override;

    bool empty(const Context & context) const override;


    void attachTable(const String & table_name, const StoragePtr & table) override;

    StoragePtr detachTable(const String & table_name) override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    void shutdown() override;

    virtual ~DatabaseWithOwnTablesBase() override;

protected:
    String name;

    mutable std::mutex mutex;
    Tables tables;

    DatabaseWithOwnTablesBase(String name_) : name(std::move(name_)) { }
};

}
