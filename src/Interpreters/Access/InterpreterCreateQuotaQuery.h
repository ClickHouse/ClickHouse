#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateQuotaQuery;
struct Quota;

class InterpreterCreateQuotaQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateQuotaQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

    static void updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query);

private:
    ASTPtr query_ptr;
};

}
