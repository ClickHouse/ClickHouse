#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Check that table exists. Return single row with single column "result" of type UInt8 and value 0 or 1.
  */
class InterpreterExistsQuery : public IInterpreter
{
public:
    InterpreterExistsQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
};


}
