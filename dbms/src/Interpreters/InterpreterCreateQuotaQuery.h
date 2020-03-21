#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <optional>


namespace DB
{
class ASTCreateQuotaQuery;
struct Quota;
struct GenericRoleSet;


class InterpreterCreateQuotaQuery : public IInterpreter
{
public:
    InterpreterCreateQuotaQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    void updateQuotaFromQuery(Quota & quota, const ASTCreateQuotaQuery & query, const std::optional<GenericRoleSet> & roles_from_query);

    ASTPtr query_ptr;
    Context & context;
};
}
