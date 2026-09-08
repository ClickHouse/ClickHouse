#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTWithElement.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

/// Materialized CTEs are supported by the analyzer only. Throws for a `WITH` element declared `AS MATERIALIZED`
/// anywhere in the query, so the old analyzer never inlines it silently.
class RejectMaterializedCTEMatcher
{
public:
    struct Data {};

    static void visit(const ASTPtr & ast, Data &)
    {
        const auto * with_element = ast->as<ASTWithElement>();
        if (with_element && with_element->is_materialized)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "CTE `{}` is declared `AS MATERIALIZED`, but materialized CTEs require the analyzer, which is not used for this query. "
                "Disable setting `force_materialized_cte` to inline it as a regular CTE",
                with_element->name);
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RejectMaterializedCTEVisitor = ConstInDepthNodeVisitor<RejectMaterializedCTEMatcher, true>;

}
