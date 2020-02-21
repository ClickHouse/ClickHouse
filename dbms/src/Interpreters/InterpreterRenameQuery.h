#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
    InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_);
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};


}
