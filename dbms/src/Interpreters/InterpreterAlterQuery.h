#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
class AccessRightsElements;


/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
    InterpreterAlterQuery(ASTPtr query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;

    const Context & context;
};

}
