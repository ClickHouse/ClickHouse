#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateQuotaQuery;
struct Quota;


class InterpreterCreateQuotaQuery : public IInterpreter
{
public:
    InterpreterCreateQuotaQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    void updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query);

    ASTPtr query_ptr;
    Context & context;
};
}
