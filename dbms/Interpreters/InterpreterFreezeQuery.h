#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;

class InterpreterFreezeQuery : public IInterpreter
{
public:
    InterpreterFreezeQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;
    Context & context;

    BlockInputStreamPtr executeImpl();
};

}
