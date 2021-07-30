#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return list of currently executing queries.
  */
class InterpreterShowProcesslistQuery : public IInterpreter
{
public:
    InterpreterShowProcesslistQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};


}
