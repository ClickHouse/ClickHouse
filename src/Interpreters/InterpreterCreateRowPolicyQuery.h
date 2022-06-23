#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateRowPolicyQuery;
struct RowPolicy;

class InterpreterCreateRowPolicyQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateRowPolicyQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query);

private:
    ASTPtr query_ptr;
};

}
