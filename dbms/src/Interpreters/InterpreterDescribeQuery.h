#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return names, types and other information about columns in specified table.
  */
class InterpreterDescribeQuery : public IInterpreter
{
public:
    InterpreterDescribeQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
};


}
