#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;

/** Allow to either drop table with all its data (DROP),
  * or remove information about table (just forget) from server (DETACH),
  * or just clear all data in table (TRUNCATE).
  */
class InterpreterDropQuery : public IInterpreter
{
public:
    InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    void checkAccess(const ASTDropQuery & drop);
    ASTPtr query_ptr;
    Context & context;

    BlockIO executeToDatabase(const String & database_name, ASTDropQuery::Kind kind, bool if_exists);

    BlockIO executeToTable(const String & database_name, const String & table_name, ASTDropQuery::Kind kind, bool if_exists, bool if_temporary, bool no_ddl_lock);

    BlockIO executeToDictionary(const String & database_name, const String & table_name, ASTDropQuery::Kind kind, bool if_exists, bool if_temporary, bool no_ddl_lock);

    DatabasePtr tryGetDatabase(const String & database_name, bool exists);

    DatabaseAndTable tryGetDatabaseAndTable(const String & database_name, const String & table_name, bool if_exists);

    BlockIO executeToTemporaryTable(const String & table_name, ASTDropQuery::Kind kind);
};
}
