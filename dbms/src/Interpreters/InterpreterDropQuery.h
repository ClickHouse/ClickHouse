#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>
#include <Databases/IDatabase.h>



namespace DB
{
class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
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

    BlockIO executeToDatabase(String & database_name, ASTDropQuery::Kind kind, bool if_exists);

    BlockIO executeToTable(String & database_name, String & table_name, ASTDropQuery::Kind kind, bool if_exists, bool if_temporary);

    DatabasePtr tryGetDatabase(String & database_name, bool exists);

    DatabaseAndTable tryGetDatabaseAndTable(String & database_name, String & table_name, bool if_exists);

    BlockIO executeToTemporaryTable(String & table_name, ASTDropQuery::Kind kind);
};
}
