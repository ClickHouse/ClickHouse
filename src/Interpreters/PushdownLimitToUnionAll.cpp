#include <Interpreters/PushdownLimitToUnionAll.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/typeid_cast.h>

namespace DB
{
void PushdownLimitToUnionAllMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTSelectQuery>())
        visit(*func, data);
}

void PushdownLimitToUnionAllMatcher::visit(ASTSelectQuery & select, Data &)
{
    /// When there are filter condition in SelectQuery, we can't
    /// pushdown LIMIT to UNION ALL query.
    if (select.prewhere() || select.where() || select.having())
        return;

    const auto & limit_length_ast = select.limitLength();
    auto * tables = select.tables()->as<ASTTablesInSelectQuery>();

    /// When there are no LIMIT or having more than one tables in
    /// SelectQuery(JOIN query), nothing need to do.
    if (!limit_length_ast || !tables || tables->children.size() != 1)
        return;

    auto & table = tables->children.at(0)->as<ASTTablesInSelectQueryElement &>().table_expression->as<ASTTableExpression &>();

    if (table.subquery)
    {
        auto & select_with_union_query = table.subquery->children[0]->as<ASTSelectWithUnionQuery &>();

        /// After NormalizeSelectWithUnionQueryVisitor, the height of the AST of SelectWithUnionQuery
        /// is at most 2, and if height is 2, the child SelectWithUnionQuery must be UNION DISTINCT.
        /// So, we just need pushdown LIMIT to child SelectQuery.
        if (select_with_union_query.union_mode == ASTSelectWithUnionQuery::Mode::ALL)
        {
            UInt64 limit_length = limit_length_ast->as<ASTLiteral &>().value.safeGet<UInt64>();

            for (auto & child : select_with_union_query.list_of_selects->children)
            {
                if (auto * select_query = child->as<ASTSelectQuery>())
                {
                    UInt64 old_limit_length = 0;
                    const auto old_limit_length_ast = select_query->getExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, false);

                    if (old_limit_length_ast)
                        old_limit_length = old_limit_length_ast->as<ASTLiteral &>().value.safeGet<UInt64>();

                    /// Pushdown LIMIT to UNION ALL select query when there are no LIMIT or having larger LIMIT length
                    if (!old_limit_length || old_limit_length > limit_length)
                    {
                        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, limit_length_ast->clone());
                    }
                }
            }
        }
    }
}
}
