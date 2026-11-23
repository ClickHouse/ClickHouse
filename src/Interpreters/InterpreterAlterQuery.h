#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class AccessRightsElements;
class ASTAlterCommand;
class ASTAlterQuery;


/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// `engine` is the engine of the table, empty for `ON CLUSTER`.
    static AccessRightsElements getRequiredAccessForCommand(
        const ASTAlterCommand & command, const String & database, const String & table, const String & engine = {});

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

    bool supportsTransactions() const override { return true; }

private:
    /// `engine` is the engine of the table, empty if the table is not known.
    AccessRightsElements getRequiredAccess(const String & engine = {}) const;

    BlockIO executeToTable(const ASTAlterQuery & alter);

    BlockIO executeToDatabase(const ASTAlterQuery & alter);

    ASTPtr query_ptr;
};

}
