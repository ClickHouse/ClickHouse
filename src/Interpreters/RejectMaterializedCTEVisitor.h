#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTWithElement.h>
#include <Common/Exception.h>

#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

/// Throws for a `WITH` element declared `AS MATERIALIZED` anywhere in the query, so a path that cannot
/// materialize it (old analyzer, stored view definition, lightweight `UPDATE`) never inlines it silently.
class RejectMaterializedCTEMatcher
{
public:
    struct Data
    {
        std::string_view reason;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        const auto * with_element = ast->as<ASTWithElement>();
        if (with_element && with_element->is_materialized)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "CTE `{}` is declared `AS MATERIALIZED`, but materialized CTEs {}. "
                "Disable setting `force_materialized_cte` to inline it as a regular CTE",
                with_element->name, data.reason);
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RejectMaterializedCTEVisitor = ConstInDepthNodeVisitor<RejectMaterializedCTEMatcher, true>;

/// Whether `force_materialized_cte` applies to a DDL statement on this server: it does for a user's own statement
/// and for the initial execution of a Replicated database entry, not for the replay of an already committed
/// entry, which may come from an initiator that predates the setting.
bool shouldRejectMaterializedCTE(const ContextPtr & context);

}
