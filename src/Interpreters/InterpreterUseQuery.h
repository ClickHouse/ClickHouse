#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Change default database for session.
  */
class InterpreterUseQuery : public IInterpreter, WithContext
{
public:
    InterpreterUseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    /// Checks the access to select `new_database` as the current database and that it denotes something. A
    /// hierarchical name (`USE a.b`) may denote the tables `b.*` of the database `a`: the access is checked for the
    /// database it denotes; when it is only a prefix of database names, or denotes nothing, it is checked as written.
    /// The access is checked before a name that denotes nothing is reported, so that it does not tell a user who is
    /// not allowed to see a database whether it exists. Shared with the MySQL `COM_INIT_DB` command.
    static void checkAccessAndExistence(const String & new_database, const ContextPtr & local_context);

private:
    ASTPtr query_ptr;
};

}
