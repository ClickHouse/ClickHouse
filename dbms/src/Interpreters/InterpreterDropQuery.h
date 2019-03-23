#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
using DatabaseAndDictionary = std::pair<DatabasePtr, DictionaryPtr>;


/** Allow to either drop table with all its data (DROP),
  * or remove information about table or dictionary (just forget) from server (DETACH),
  * or just clear all data in table (TRUNCATE).
  */
class InterpreterDropQuery : public IInterpreter
{
public:
    InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_);

    /// Drop table, database, or dictionary.
    BlockIO execute() override;

private:
    void checkAccess(const ASTDropQuery & drop);
    ASTPtr query_ptr;
    Context & context;

    BlockIO executeToDatabase(String & database_name, ASTDropQuery::Kind kind, bool if_exists);

    BlockIO executeToTable(String & database_name, String & table_name, ASTDropQuery::Kind kind, bool if_exists, bool if_temporary, bool no_ddl_lock);

    BlockIO executeToDictionary(String & database_name, String & dictionary_name, ASTDropQuery::Kind kind, bool if_exists);

    DatabasePtr tryGetDatabase(const String & database_name, bool exists);

    DictionaryPtr tryGetDictionary(const DatabasePtr & database_ptr, const String & dictionary_name, bool if_exists);

    DatabaseAndTable tryGetDatabaseAndTable(String & database_name, String & table_name, bool if_exists);

    DatabaseAndDictionary tryGetDatabaseAndDictionary(String & database_name, String & dictionary_name, bool if_exists);

    BlockIO executeToTemporaryTable(String & table_name, ASTDropQuery::Kind kind);
};
}
