#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateQueryMaskingRuleQuery;
struct QueryMaskingRule;

class InterpreterCreateQueryMaskingRuleQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateQueryMaskingRuleQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

    /// Also validates the resulting rule, so an invalid rule is rejected wherever it comes from.
    static void updateQueryMaskingRuleFromQuery(QueryMaskingRule & rule, const ASTCreateQueryMaskingRuleQuery & query);

private:
    ASTPtr query_ptr;
};

}
