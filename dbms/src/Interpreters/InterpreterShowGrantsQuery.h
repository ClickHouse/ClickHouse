#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;


/** Return a single column containing granted privileges for a specified role.
  */
class InterpreterShowGrantsQuery : public IInterpreter
{
public:
    InterpreterShowGrantsQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
};


}
