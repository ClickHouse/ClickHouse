#include <Interpreters/RewriteUniqToCountVisitor.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;

namespace
{

bool matchFnUniq(String name)
{
    return name == "uniq" || name == "uniqHLL12" || name == "uniqExact" || name == "uniqTheta" || name == "uniqCombined"
        || name == "uniqCombined64";
}

bool expressionEquals(const ASTPtr & lhs, const ASTPtr & rhs, const Aliases & alias)
{
    if (lhs->getTreeHash(/*ignore_aliases=*/ true) == rhs->getTreeHash(/*ignore_aliases=*/ true))
        return true;

    auto * lhs_idf = lhs->as<ASTIdentifier>();
    auto * rhs_idf = rhs->as<ASTIdentifier>();
    if (lhs_idf && rhs_idf)
    {
        /// compound identifiers, such as: <t.name, name>
        if (lhs_idf->shortName() == rhs_idf->shortName())
            return true;

        /// translate alias
        if (alias.find(lhs_idf->shortName()) != alias.end())
            lhs_idf = alias.find(lhs_idf->shortName())->second->as<ASTIdentifier>();

        if (alias.find(rhs_idf->shortName()) != alias.end())
            rhs_idf = alias.find(rhs_idf->shortName())->second->as<ASTIdentifier>();

        if (lhs_idf && rhs_idf && lhs_idf->shortName() == rhs_idf->shortName())
            return true;
    }

    return false;
}

bool expressionListEquals(ASTExpressionList * lhs, ASTExpressionList * rhs, const Aliases & alias)
{
    if (!lhs || !rhs)
        return false;
    if (lhs->children.size() != rhs->children.size())
        return false;
    for (size_t i = 0; i < lhs->children.size(); i++)
    {
        if (!expressionEquals(lhs->children[i], rhs->children[i], alias))
            return false;
    }
    return true;
}

/// Test whether lhs contains all expressions in rhs.
bool expressionListContainsAll(ASTExpressionList * lhs, ASTExpressionList * rhs, const Aliases & alias)
{
    if (!lhs || !rhs)
        return false;
    if (lhs->children.size() < rhs->children.size())
        return false;
    for (const auto & re : rhs->children)
    {
        auto predicate = [&re, &alias](ASTPtr & le) { return expressionEquals(le, re, alias); };
        if (std::find_if(lhs->children.begin(), lhs->children.end(), predicate) == lhs->children.end())
            return false;
    }
    return true;
}

}

void RewriteUniqToCountMatcher::visit(ASTPtr & ast, Data & /*data*/)
{
    auto * selectq = ast->as<ASTSelectQuery>();
    if (!selectq || !selectq->tables() || selectq->tables()->children.size() != 1)
        return;
    auto expr_list = selectq->select();
    if (!expr_list || expr_list->children.size() != 1)
        return;
    auto * func = expr_list->children[0]->as<ASTFunction>();
    if (!func || !matchFnUniq(func->name))
        return;
    if (selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children.size() != 1)
        return;
    auto * table_expr = selectq->tables()
                            ->as<ASTTablesInSelectQuery>()
                            ->children[0]
                            ->as<ASTTablesInSelectQueryElement>()
                            ->children[0]
                            ->as<ASTTableExpression>();
    if (!table_expr || table_expr->children.size() != 1 || !table_expr->subquery)
        return;
    auto * subquery = table_expr->subquery->as<ASTSubquery>();
    if (!subquery)
        return;
    auto * sub_selectq = subquery->children[0]
                             ->as<ASTSelectWithUnionQuery>()->children[0]
                             ->as<ASTExpressionList>()->children[0]
                             ->as<ASTSelectQuery>();
    if (!sub_selectq)
        return;
    auto sub_expr_list = sub_selectq->select();
    if (!sub_expr_list)
        return;

    /// collect subquery select expressions alias
    Aliases alias;
    for (const auto & expr : sub_expr_list->children)
    {
        if (!expr->tryGetAlias().empty())
            alias.insert({expr->tryGetAlias(), expr});
    }

    /// Whether query matches 'SELECT uniq(x ...) FROM (SELECT DISTINCT x ...)'
    auto match_subquery_with_distinct = [&]() -> bool
    {
        if (!sub_selectq->distinct)
            return false;
        /// uniq expression list == subquery group by expression list
        if (!expressionListEquals(func->children[0]->as<ASTExpressionList>(), sub_expr_list->as<ASTExpressionList>(), alias))
            return false;
        return true;
    };

    /// Whether query matches 'SELECT uniq(x ...) FROM (SELECT x ... GROUP BY x ...)'
    auto match_subquery_with_group_by = [&]() -> bool
    {
        auto group_by = sub_selectq->groupBy();
        if (!group_by)
            return false;
        /// uniq expression list == subquery group by expression list
        if (!expressionListEquals(func->children[0]->as<ASTExpressionList>(), group_by->as<ASTExpressionList>(), alias))
            return false;
        /// subquery select expression list must contain all columns in uniq expression list
        if (!expressionListContainsAll(sub_expr_list->as<ASTExpressionList>(), func->children[0]->as<ASTExpressionList>(), alias))
            return false;
        return true;
    };

    if (match_subquery_with_distinct() || match_subquery_with_group_by())
    {
        auto main_alias = expr_list->children[0]->tryGetAlias();
        expr_list->children[0] = makeASTFunction("count");
        expr_list->children[0]->setAlias(main_alias);
    }
}

}
