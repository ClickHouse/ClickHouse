#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterCreateRewriteRuleQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateRewriteRuleQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
