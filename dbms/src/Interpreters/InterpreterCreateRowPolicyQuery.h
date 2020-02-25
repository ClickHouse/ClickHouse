#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <optional>


namespace DB
{
class ASTCreateRowPolicyQuery;
struct RowPolicy;
struct GenericRoleSet;


class InterpreterCreateRowPolicyQuery : public IInterpreter
{
public:
    InterpreterCreateRowPolicyQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    void updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query, const std::optional<GenericRoleSet> & roles_from_query);

    ASTPtr query_ptr;
    Context & context;
};
}
