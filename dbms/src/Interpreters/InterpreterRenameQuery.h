#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{


/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
    InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_);
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context context;
};


}
