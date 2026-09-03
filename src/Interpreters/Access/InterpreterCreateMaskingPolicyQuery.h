#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateMaskingPolicyQuery;
class AccessRightsElements;
struct MaskingPolicy;

class InterpreterCreateMaskingPolicyQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateMaskingPolicyQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
