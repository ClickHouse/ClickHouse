#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class InterpreterDropAccessQuery : public IInterpreter
{
public:
    InterpreterDropAccessQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};
}
