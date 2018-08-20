#include <Common/typeid_cast.h>
#include <Storages/IStorage.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <iostream>

namespace DB
{

static constexpr auto and_function_name = "and";

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    ASTSelectQuery * ast_select_, const Settings & settings_)
    : ast_select(ast_select_), settings(settings_)
{
}

bool PredicateExpressionsOptimizer::optimize()
{
    if (!settings.enable_optimize_predicate_expression || !ast_select || !ast_select->tables)
        return false;

    SubQueriesProjectionColumns all_subquery_projection_columns;
    getAllSubqueryProjectionColumns(ast_select->tables.get(), all_subquery_projection_columns);

    bool is_rewrite_sub_queries = false;
    if (!all_subquery_projection_columns.empty())
    {
        is_rewrite_sub_queries |= optimizeImpl(ast_select->where_expression, all_subquery_projection_columns, false);
        is_rewrite_sub_queries |= optimizeImpl(ast_select->prewhere_expression, all_subquery_projection_columns, true);
    }
    return is_rewrite_sub_queries;
}

bool PredicateExpressionsOptimizer::optimizeImpl(
    ASTPtr & outer_expression, SubQueriesProjectionColumns & sub_queries_projection_columns, bool is_prewhere)
{
    /// split predicate with `and`
    PredicateExpressions outer_predicate_expressions = splitConjunctionPredicate(outer_expression);

    bool is_rewrite_subquery = false;
    for (const auto & outer_predicate : outer_predicate_expressions)
    {
        ASTs outer_predicate_dependent;
        getExpressionDependentColumns(outer_predicate, outer_predicate_dependent);

        /// TODO: remove origin expression
        for (const auto & subquery_projection_columns : sub_queries_projection_columns)
        {
            auto subquery = static_cast<ASTSelectQuery *>(subquery_projection_columns.first);
            const ProjectionsWithAliases projection_columns = subquery_projection_columns.second;

            OptimizeKind optimize_kind = OptimizeKind::NONE;
            if (!cannotPushDownOuterPredicate(projection_columns, subquery, outer_predicate_dependent, is_prewhere, optimize_kind))
            {
                ASTPtr inner_predicate;
                cloneOuterPredicateForInnerPredicate(outer_predicate, projection_columns, outer_predicate_dependent, inner_predicate);

                switch(optimize_kind)
                {
                    case OptimizeKind::NONE: continue;
                    case OptimizeKind::PUSH_TO_WHERE: is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery->where_expression, subquery); continue;
                    case OptimizeKind::PUSH_TO_HAVING: is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery->having_expression, subquery); continue;
                    case OptimizeKind::PUSH_TO_PREWHERE: is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery->prewhere_expression, subquery); continue;
                }
            }
        }
    }
    return is_rewrite_subquery;
}

PredicateExpressions PredicateExpressionsOptimizer::splitConjunctionPredicate(ASTPtr & predicate_expression)
{
    PredicateExpressions predicate_expressions;

    if (predicate_expression)
    {
        predicate_expressions.emplace_back(predicate_expression);

        auto remove_expression_at_index = [&predicate_expressions] (const size_t index)
        {
            if (index < predicate_expressions.size() - 1)
                std::swap(predicate_expressions[index], predicate_expressions.back());
            predicate_expressions.pop_back();
        };

        for (size_t idx = 0; idx < predicate_expressions.size();)
        {
            const auto expression = predicate_expressions.at(idx);

            if (const auto function = typeid_cast<ASTFunction *>(expression.get()))
            {
                if (function->name == and_function_name)
                {
                    for (auto & child : function->arguments->children)
                        predicate_expressions.emplace_back(child);

                    remove_expression_at_index(idx);
                    continue;
                }
            }
            idx++;
        }
    }
    return predicate_expressions;
}

void PredicateExpressionsOptimizer::getExpressionDependentColumns(const ASTPtr & expression, ASTs & expression_dependent_columns)
{
    if (!typeid_cast<ASTIdentifier *>(expression.get()))
    {
        for (const auto & child : expression->children)
            getExpressionDependentColumns(child, expression_dependent_columns);

        return;
    }

    expression_dependent_columns.emplace_back(expression);
}

bool PredicateExpressionsOptimizer::cannotPushDownOuterPredicate(
    const ProjectionsWithAliases & subquery_projection_columns, ASTSelectQuery * subquery,
    ASTs & expression_dependent_columns, bool & is_prewhere, OptimizeKind & optimize_kind)
{
    if (subquery->final() || subquery->limit_by_expression_list || subquery->limit_offset || subquery->with_expression_list)
        return true;

    for (auto & dependent_column : expression_dependent_columns)
    {
        bool is_found = false;
        String dependent_column_name = dependent_column->getAliasOrColumnName();

        for (auto projection_column : subquery_projection_columns)
        {
            if (projection_column.second == dependent_column_name)
            {
                is_found = true;
                optimize_kind = isAggregateFunction(projection_column.first) ? OptimizeKind::PUSH_TO_HAVING : optimize_kind;
            }
        }

        if (!is_found)
            return true;
    }

    if (optimize_kind == OptimizeKind::NONE)
        optimize_kind = is_prewhere ? OptimizeKind::PUSH_TO_PREWHERE : OptimizeKind::PUSH_TO_WHERE;

    return false;
}

bool PredicateExpressionsOptimizer::isAggregateFunction(ASTPtr & node)
{
    if (auto function = typeid_cast<ASTFunction *>(node.get()))
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
            return true;
    }

    for (auto & child : node->children)
        if (isAggregateFunction(child))
            return true;

    return false;
}

void PredicateExpressionsOptimizer::getAllSubqueryProjectionColumns(IAST * node, SubQueriesProjectionColumns & all_subquery_projection_columns)
{
    if (auto ast_subquery = typeid_cast<ASTSubquery *>(node))
    {
        ASTs output_projection;
        IAST * subquery = ast_subquery->children.at(0).get();
        getSubqueryProjectionColumns(subquery, all_subquery_projection_columns, output_projection);
        return;
    }

    for (auto & child : node->children)
        getAllSubqueryProjectionColumns(child.get(), all_subquery_projection_columns);
}

void PredicateExpressionsOptimizer::cloneOuterPredicateForInnerPredicate(
    const ASTPtr & outer_predicate, const ProjectionsWithAliases & projection_columns, ASTs & predicate_dependent_columns,
    ASTPtr & inner_predicate)
{
    inner_predicate = outer_predicate->clone();

    ASTs new_expression_require_columns;
    new_expression_require_columns.reserve(predicate_dependent_columns.size());
    getExpressionDependentColumns(inner_predicate, new_expression_require_columns);

    for (auto & expression : new_expression_require_columns)
    {
        if (auto identifier = typeid_cast<ASTIdentifier *>(expression.get()))
        {
            for (auto projection : projection_columns)
            {
                if (identifier->name == projection.second)
                    identifier->name = projection.first->getAliasOrColumnName();
            }
        }
    }
}

bool PredicateExpressionsOptimizer::optimizeExpression(const ASTPtr & outer_expression, ASTPtr & subquery_expression, ASTSelectQuery * subquery)
{
    ASTPtr new_subquery_expression = subquery_expression;
    new_subquery_expression = new_subquery_expression ? makeASTFunction(and_function_name, outer_expression, subquery_expression) : outer_expression;

    if (!subquery_expression)
        subquery->children.emplace_back(new_subquery_expression);
    else
        for (auto & child : subquery->children)
            if (child == subquery_expression)
                child = new_subquery_expression;

    subquery_expression = std::move(new_subquery_expression);
    return true;
}

void PredicateExpressionsOptimizer::getSubqueryProjectionColumns(IAST * subquery, SubQueriesProjectionColumns & all_subquery_projection_columns, ASTs & output_projections)
{
    if (auto * with_union_subquery = typeid_cast<ASTSelectWithUnionQuery *>(subquery))
        for (auto & select : with_union_subquery->list_of_selects->children)
            getSubqueryProjectionColumns(select.get(), all_subquery_projection_columns, output_projections);


    if (auto * without_union_subquery = typeid_cast<ASTSelectQuery *>(subquery))
    {
        const auto expression_list = without_union_subquery->select_expression_list->children;

        /// use first projection as the output projection
        if (output_projections.empty())
            output_projections = expression_list;

        if (output_projections.size() != expression_list.size())
            throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

        ProjectionsWithAliases subquery_projections;
        subquery_projections.reserve(expression_list.size());

        for (size_t idx = 0; idx < expression_list.size(); idx++)
            subquery_projections.emplace_back(std::pair(expression_list.at(idx), output_projections.at(idx)->getAliasOrColumnName()));

        all_subquery_projection_columns.insert(std::pair(subquery, subquery_projections));
    }
}

}
