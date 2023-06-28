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

namespace
{

bool matchFnUniq(String func_name)
{
    auto name = Poco::toLower(func_name);
    return name == "uniq" || name == "uniqHLL12" || name == "uniqExact" || name == "uniqTheta" || name == "uniqCombined" || name == "uniqCombined64";
}

bool expressionListEquals(ASTExpressionList * lhs, ASTExpressionList * rhs)
{
    if (!lhs || !rhs)
        return false;
    if (lhs->children.size() != rhs->children.size())
        return false;
    for (size_t i = 0; i < lhs->children.size(); i++)
    {
        if (lhs->children[i]->formatForLogging() != rhs->children[i]->formatForLogging()) // TODO not a elegant way
            return false;
    }
    return true;
}

/// Test whether lhs contains all expr in rhs.
bool expressionListContainsAll(ASTExpressionList * lhs, ASTExpressionList * rhs)
{
    if (!lhs || !rhs)
        return false;
    if (lhs->children.size() < rhs->children.size())
        return false;
    std::vector<String> lhs_strs;
    for (const auto & le : lhs->children)
    {
        lhs_strs.emplace_back(le->formatForLogging());
    }
    for (const auto & re : rhs->children)
    {
        if (std::find(lhs_strs.begin(), lhs_strs.end(), re->formatForLogging()) != lhs_strs.end())
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
    auto * table_expr = selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children[0]->as<ASTTableExpression>();
    if (!table_expr || table_expr->children.size() != 1 || !table_expr->subquery)
        return;
    auto * subquery = table_expr->subquery->as<ASTSubquery>();
    if (!subquery)
        return;
    auto * sub_selectq = subquery->children[0]->as<ASTSelectWithUnionQuery>()->children[0]->as<ASTExpressionList>()->children[0]->as<ASTSelectQuery>();
    if (!sub_selectq)
        return;

    auto match_distinct = [&]() -> bool
    {
        if (!sub_selectq->distinct)
            return false;
        auto sub_expr_list = sub_selectq->select();
        if (!sub_expr_list)
            return false;
        /// uniq expression list == subquery group by expression list
        if (!expressionListEquals(func->children[0]->as<ASTExpressionList>(), sub_expr_list->as<ASTExpressionList>()))
            return false;
        return true;
    };

    auto match_group_by = [&]() -> bool
    {
        auto group_by = sub_selectq->groupBy();
        if (!group_by)
            return false;
        auto sub_expr_list = sub_selectq->select();
        if (!sub_expr_list)
            return false;
        /// uniq expression list == subquery group by expression list 
        if (!expressionListEquals(func->children[0]->as<ASTExpressionList>(), group_by->as<ASTExpressionList>()))
            return false;
        /// subquery select expression list must contain all columns in uniq expression list
        expressionListContainsAll(sub_expr_list->as<ASTExpressionList>(), func->children[0]->as<ASTExpressionList>());
        return true;
    };

    if (match_distinct() || match_group_by())
        expr_list->children[0] = makeASTFunction("count");
}

}
