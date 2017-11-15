#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


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
