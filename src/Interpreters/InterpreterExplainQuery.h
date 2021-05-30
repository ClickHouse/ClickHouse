#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_) : WithContext(context_), query(query_) { }

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query;

    BlockInputStreamPtr executeImpl();
};


}
