#include <iostream>

#include <Common/typeid_cast.h>
#include <Storages/IStorage.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/PredicateRewriteVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    const Context & context_, const TablesWithColumnNames & tables_with_columns_, const Settings & settings_)
    : context(context_), tables_with_columns(tables_with_columns_), settings(settings_)
{
}

bool PredicateExpressionsOptimizer::optimize(ASTSelectQuery & select_query)
{
    if (!settings.enable_optimize_predicate_expression || !select_query.tables() || select_query.tables()->children.empty())
        return false;

    if ((!select_query.where() && !select_query.prewhere()) || select_query.array_join_expression_list())
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
            else if (expression_info.unique_reference_tables_pos.size() == 0)
            {
                for (size_t index = 0; index < tables_predicates.size(); ++index)
                    tables_predicates[index].emplace_back(predicate_expression);
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
        PredicateRewriteVisitor::Data data(
            context, table_predicates, table_column, settings.enable_optimize_predicate_expression_to_final_subquery);

        PredicateRewriteVisitor(data).visit(table_element);
        return data.isRewrite();
    }

    return false;
}

}
