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
class DatabaseSnaphotIterator : public IDatabaseIterator
{
private:
    Tables tables;
    Tables::iterator it;

public:
    DatabaseSnaphotIterator(Tables & tables_)
        : tables(tables_), it(tables.begin()) {}

    DatabaseSnaphotIterator(Tables && tables_)
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

}
