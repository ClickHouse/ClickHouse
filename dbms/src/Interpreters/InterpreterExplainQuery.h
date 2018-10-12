#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, const Context & )
        : query(query_)
    {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query;

    BlockInputStreamPtr executeImpl();
};


}
