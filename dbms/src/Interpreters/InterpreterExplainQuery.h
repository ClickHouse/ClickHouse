#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, const Context & context_)
        : query(query_), context(context_)
    {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query;
    Context context;

    BlockInputStreamPtr executeImpl();
};


}
