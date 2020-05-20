#include <Interpreters/PredicateExpressionsOptimizer.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/PredicateRewriteVisitor.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>


namespace DB
{

namespace ErrorCodes
{
}

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    const Context & context_, const TablesWithColumnNames & tables_with_columns_, const Settings & settings_)
    : context(context_), tables_with_columns(tables_with_columns_), settings(settings_)
{
}

bool PredicateExpressionsOptimizer::optimize(ASTSelectQuery & select_query)
{
    if (!settings.enable_optimize_predicate_expression)
        return false;

    if (select_query.having() && (!select_query.group_by_with_cube && !select_query.group_by_with_rollup && !select_query.group_by_with_totals))
        tryMovePredicatesFromHavingToWhere(select_query);

    if (!select_query.tables() || select_query.tables()->children.empty())
        return false;

    if ((!select_query.where() && !select_query.prewhere()) || select_query.arrayJoinExpressionList())
        return false;

    const auto & tables_predicates = extractTablesPredicates(select_query.where(), select_query.prewhere());

    if (!tables_predicates.empty())
        return tryRewritePredicatesToTables(select_query.refTables()->children, tables_predicates);

    return false;
}

static ASTs splitConjunctionPredicate(const std::initializer_list<const ASTPtr> & predicates)
{
    std::vector<ASTPtr> res;

    auto remove_expression_at_index = [&res] (const size_t index)
    {
        if (index < res.size() - 1)
            std::swap(res[index], res.back());
        res.pop_back();
    };

    for (const auto & predicate : predicates)
    {
        if (!predicate)
            continue;

        res.emplace_back(predicate);

        for (size_t idx = 0; idx < res.size();)
        {
            const auto & expression = res.at(idx);

            if (const auto * function = expression->as<ASTFunction>(); function && function->name == "and")
            {
                for (auto & child : function->arguments->children)
                    res.emplace_back(child);

                remove_expression_at_index(idx);
                continue;
            }
            ++idx;
        }
    }

    return res;
}

std::vector<ASTs> PredicateExpressionsOptimizer::extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere)
{
    std::vector<ASTs> tables_predicates(tables_with_columns.size());

    for (const auto & predicate_expression : splitConjunctionPredicate({where, prewhere}))
    {
        ExpressionInfoVisitor::Data expression_info{.context = context, .tables = tables_with_columns};
        ExpressionInfoVisitor(expression_info).visit(predicate_expression);

        if (expression_info.is_stateful_function)
            return {};   /// give up the optimization when the predicate contains stateful function

        if (!expression_info.is_array_join)
        {
            if (expression_info.unique_reference_tables_pos.size() == 1)
                tables_predicates[*expression_info.unique_reference_tables_pos.begin()].emplace_back(predicate_expression);
            else if (expression_info.unique_reference_tables_pos.empty())
            {
                for (auto & predicate : tables_predicates)
                    predicate.emplace_back(predicate_expression);
            }
        }
    }

    return tables_predicates;    /// everything is OK, it can be optimized
}

bool PredicateExpressionsOptimizer::tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates)
{
    bool is_rewrite_tables = false;

    for (size_t index = tables_element.size(); index > 0; --index)
    {
        size_t table_pos = index - 1;

        /// NOTE: the syntactic way of pushdown has limitations and should be partially disabled in case of JOINs.
        ///       Let's take a look at the query:
        ///
        ///           SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0
        ///
        ///       The result is empty - without pushdown. But the pushdown tends to modify it in this way:
        ///
        ///           SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b WHERE b = 0) USING (a) WHERE b = 0
        ///
        ///       That leads to the empty result in the right subquery and changes the whole outcome to (1, 0) or (1, NULL).
        ///       It happens because the not-matching columns are replaced with a global default values on JOIN.
        ///       Same is true for RIGHT JOIN and FULL JOIN.

        if (const auto & table_element = tables_element[table_pos]->as<ASTTablesInSelectQueryElement>())
        {
            if (table_element->table_join && isLeft(table_element->table_join->as<ASTTableJoin>()->kind))
                continue;  /// Skip right table optimization

            if (table_element->table_join && isFull(table_element->table_join->as<ASTTableJoin>()->kind))
                break;  /// Skip left and right table optimization

            is_rewrite_tables |= tryRewritePredicatesToTable(tables_element[table_pos], tables_predicates[table_pos],
                tables_with_columns[table_pos].columns);

            if (table_element->table_join && isRight(table_element->table_join->as<ASTTableJoin>()->kind))
                break;  /// Skip left table optimization
        }
    }

    return is_rewrite_tables;
}

bool PredicateExpressionsOptimizer::tryRewritePredicatesToTable(ASTPtr & table_element, const ASTs & table_predicates, const Names & table_column) const
{
    if (!table_predicates.empty())
    {
        auto optimize_final = settings.enable_optimize_predicate_expression_to_final_subquery;
        PredicateRewriteVisitor::Data data(context, table_predicates, table_column, optimize_final);

        PredicateRewriteVisitor(data).visit(table_element);
        return data.is_rewrite;
    }

    return false;
}

bool PredicateExpressionsOptimizer::tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query)
{
    ASTs where_predicates;
    ASTs having_predicates;

    const auto & reduce_predicates = [&](const ASTs & predicates)
    {
        ASTPtr res = predicates[0];
        for (size_t index = 1; index < predicates.size(); ++index)
            res = makeASTFunction("and", res, predicates[index]);

        return res;
    };

    for (const auto & moving_predicate: splitConjunctionPredicate({select_query.having()}))
    {
        ExpressionInfoVisitor::Data expression_info{.context = context, .tables = {}};
        ExpressionInfoVisitor(expression_info).visit(moving_predicate);

        /// TODO: If there is no group by, where, and prewhere expression, we can push down the stateful function
        if (expression_info.is_stateful_function)
            return false;

        if (expression_info.is_aggregate_function)
            having_predicates.emplace_back(moving_predicate);
        else
            where_predicates.emplace_back(moving_predicate);
    }

    if (having_predicates.empty())
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, {});
    else
    {
        auto having_predicate = reduce_predicates(having_predicates);
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, std::move(having_predicate));
    }

    if (!where_predicates.empty())
    {
        auto moved_predicate = reduce_predicates(where_predicates);
        moved_predicate = select_query.where() ? makeASTFunction("and", select_query.where(), moved_predicate) : moved_predicate;
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(moved_predicate));
    }

    return true;
}

}
