#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class AccessRightsElements;


/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
    InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_);
    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;
    Context & context;
};

}
