#include <Common/typeid_cast.h>
#include <Storages/IStorage.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <iostream>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/queryToString.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include "TranslateQualifiedNamesVisitor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}

static constexpr auto and_function_name = "and";

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    ASTSelectQuery * ast_select_, ExtractedSettings && settings_, const Context & context_)
        : ast_select(ast_select_), settings(settings_), context(context_)
{
}

bool PredicateExpressionsOptimizer::optimize()
{
    if (!settings.enable_optimize_predicate_expression || !ast_select || !ast_select->tables || ast_select->tables->children.empty())
        return false;

    if (!ast_select->where_expression && !ast_select->prewhere_expression)
        return false;

    SubqueriesProjectionColumns all_subquery_projection_columns;
    getAllSubqueryProjectionColumns(all_subquery_projection_columns);

    bool is_rewrite_subqueries = false;
    if (!all_subquery_projection_columns.empty())
    {
        is_rewrite_subqueries |= optimizeImpl(ast_select->where_expression, all_subquery_projection_columns, false);
        is_rewrite_subqueries |= optimizeImpl(ast_select->prewhere_expression, all_subquery_projection_columns, true);
    }
    return is_rewrite_subqueries;
}

bool PredicateExpressionsOptimizer::optimizeImpl(
    ASTPtr & outer_expression, SubqueriesProjectionColumns & subqueries_projection_columns, bool is_prewhere)
{
    /// split predicate with `and`
    PredicateExpressions outer_predicate_expressions = splitConjunctionPredicate(outer_expression);

    std::vector<DatabaseAndTableWithAlias> database_and_table_with_aliases =
        getDatabaseAndTables(*ast_select, context.getCurrentDatabase());

    bool is_rewrite_subquery = false;
    for (const auto & outer_predicate : outer_predicate_expressions)
    {
        if (isArrayJoinFunction(outer_predicate))
            continue;

        IdentifiersWithQualifiedNameSet outer_predicate_dependencies;
        getDependenciesAndQualifiedOfExpression(outer_predicate, outer_predicate_dependencies, database_and_table_with_aliases);

        /// TODO: remove origin expression
        for (const auto & subquery_projection_columns : subqueries_projection_columns)
        {
            auto subquery = static_cast<ASTSelectQuery *>(subquery_projection_columns.first);
            const ProjectionsWithAliases projection_columns = subquery_projection_columns.second;

            OptimizeKind optimize_kind = OptimizeKind::NONE;
            if (!cannotPushDownOuterPredicate(projection_columns, subquery, outer_predicate_dependencies, is_prewhere, optimize_kind))
            {
                ASTPtr inner_predicate;
                cloneOuterPredicateForInnerPredicate(outer_predicate, projection_columns, database_and_table_with_aliases, inner_predicate);

                switch (optimize_kind)
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

void PredicateExpressionsOptimizer::getDependenciesAndQualifiedOfExpression(const ASTPtr & expression,
                                                                            IdentifiersWithQualifiedNameSet & dependencies_and_qualified,
                                                                            std::vector<DatabaseAndTableWithAlias> & tables_with_aliases)
{
    if (const auto identifier = typeid_cast<ASTIdentifier *>(expression.get()))
    {
        String table_alias;
        if (!identifier->name_parts.empty())
        {
            if (!tables_with_aliases.empty())
                table_alias = tables_with_aliases[0].getQualifiedNamePrefix();
        }
        else
        {
            size_t best_table_pos = 0;
            size_t max_num_qualifiers_to_strip = 0;

            /// translate qualifiers for dependent columns
            for (size_t table_pos = 0; table_pos < tables_with_aliases.size(); ++table_pos)
            {
                const auto & table = tables_with_aliases[table_pos];
                auto num_qualifiers_to_strip = getNumComponentsToStripInOrderToTranslateQualifiedName(*identifier, table);

                if (num_qualifiers_to_strip > max_num_qualifiers_to_strip)
                {
                    max_num_qualifiers_to_strip = num_qualifiers_to_strip;
                    best_table_pos = table_pos;
                }
            }

            table_alias = tables_with_aliases[best_table_pos].getQualifiedNamePrefix();
        }

        String qualified_name = table_alias + expression->getAliasOrColumnName();
        dependencies_and_qualified.emplace_back(std::pair(identifier, qualified_name));
    }
    else
    {
        for (const auto & child : expression->children)
            getDependenciesAndQualifiedOfExpression(child, dependencies_and_qualified, tables_with_aliases);
    }
}

bool PredicateExpressionsOptimizer::cannotPushDownOuterPredicate(
    const ProjectionsWithAliases & subquery_projection_columns, ASTSelectQuery * subquery,
    IdentifiersWithQualifiedNameSet & outer_predicate_dependencies, bool & is_prewhere, OptimizeKind & optimize_kind)
{
    if (subquery->final() || subquery->limit_by_expression_list || subquery->limit_length || subquery->with_expression_list)
        return true;

    for (auto & predicate_dependency : outer_predicate_dependencies)
    {
        bool is_found = false;

        for (auto projection_column : subquery_projection_columns)
        {
            if (projection_column.second == predicate_dependency.second)
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

bool PredicateExpressionsOptimizer::isArrayJoinFunction(const ASTPtr & node)
{
    if (auto function = typeid_cast<ASTFunction *>(node.get()))
    {
        if (function->name == "arrayJoin")
            return true;
    }

    for (auto & child : node->children)
        if (isArrayJoinFunction(child))
            return true;

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

void PredicateExpressionsOptimizer::cloneOuterPredicateForInnerPredicate(
    const ASTPtr & outer_predicate, const ProjectionsWithAliases & projection_columns,
    std::vector<DatabaseAndTableWithAlias> & tables, ASTPtr & inner_predicate)
{
    inner_predicate = outer_predicate->clone();

    /// clears the alias name contained in the outer predicate
    cleanExpressionAlias(inner_predicate);
    IdentifiersWithQualifiedNameSet new_expression_requires;
    getDependenciesAndQualifiedOfExpression(inner_predicate, new_expression_requires, tables);

    for (auto & require : new_expression_requires)
    {
        for (auto projection : projection_columns)
        {
            if (require.second == projection.second)
            {
                ASTPtr & ast = projection.first;
                if (!typeid_cast<ASTIdentifier *>(ast.get()) && ast->tryGetAlias().empty())
                    ast->setAlias(ast->getColumnName());
                require.first->name = ast->getAliasOrColumnName();
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

void PredicateExpressionsOptimizer::getAllSubqueryProjectionColumns(SubqueriesProjectionColumns & all_subquery_projection_columns)
{
    const auto tables_expression = getSelectTablesExpression(*ast_select);

    for (const auto & table_expression : tables_expression)
    {
        if (table_expression->subquery)
        {
            /// Use qualifiers to translate the columns of subqueries
            DatabaseAndTableWithAlias database_and_table_with_alias(*table_expression, context.getCurrentDatabase());
            String qualified_name_prefix = database_and_table_with_alias.getQualifiedNamePrefix();
            getSubqueryProjectionColumns(all_subquery_projection_columns, qualified_name_prefix,
                                         static_cast<const ASTSubquery *>(table_expression->subquery.get())->children[0]);
        }
    }
}

void PredicateExpressionsOptimizer::getSubqueryProjectionColumns(SubqueriesProjectionColumns & all_subquery_projection_columns,
                                                                 String & qualified_name_prefix, const ASTPtr & subquery)
{
    ASTs select_with_union_projections;
    auto select_with_union_query = static_cast<ASTSelectWithUnionQuery *>(subquery.get());

    for (auto & select_without_union_query : select_with_union_query->list_of_selects->children)
    {
        ProjectionsWithAliases subquery_projections;
        auto select_projection_columns = getSelectQueryProjectionColumns(select_without_union_query);

        if (!select_projection_columns.empty())
        {
            if (select_with_union_projections.empty())
                select_with_union_projections = select_projection_columns;

            for (size_t i = 0; i < select_projection_columns.size(); i++)
                subquery_projections.emplace_back(std::pair(select_projection_columns[i],
                                                            qualified_name_prefix + select_with_union_projections[i]->getAliasOrColumnName()));

            all_subquery_projection_columns.insert(std::pair(select_without_union_query.get(), subquery_projections));
        }
    }
}

ASTs PredicateExpressionsOptimizer::getSelectQueryProjectionColumns(ASTPtr & ast)
{
    ASTs projection_columns;
    auto select_query = static_cast<ASTSelectQuery *>(ast.get());

    /// first should normalize query tree.
    std::unordered_map<String, ASTPtr> aliases;
    std::vector<DatabaseAndTableWithAlias> tables = getDatabaseAndTables(*select_query, context.getCurrentDatabase());

    TranslateQualifiedNamesVisitor::Data qn_visitor_data{{}, tables};
    TranslateQualifiedNamesVisitor(qn_visitor_data).visit(ast);

    QueryAliasesVisitor::Data query_aliases_data{aliases};
    QueryAliasesVisitor(query_aliases_data).visit(ast);

    QueryNormalizer::Data normalizer_data(aliases, settings);
    QueryNormalizer(normalizer_data).visit(ast);

    for (const auto & projection_column : select_query->select_expression_list->children)
    {
        if (typeid_cast<ASTAsterisk *>(projection_column.get()) || typeid_cast<ASTQualifiedAsterisk *>(projection_column.get()))
        {
            ASTs evaluated_columns = evaluateAsterisk(select_query, projection_column);

            for (const auto & column : evaluated_columns)
                projection_columns.emplace_back(column);

            continue;
        }

        projection_columns.emplace_back(projection_column);
    }
    return projection_columns;
}

ASTs PredicateExpressionsOptimizer::evaluateAsterisk(ASTSelectQuery * select_query, const ASTPtr & asterisk)
{
    /// SELECT *, SELECT dummy, SELECT 1 AS id
    if (!select_query->tables || select_query->tables->children.empty())
        return {};

    std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(*select_query);

    if (const auto qualified_asterisk = typeid_cast<ASTQualifiedAsterisk *>(asterisk.get()))
    {
        if (qualified_asterisk->children.size() != 1)
            throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);

        DatabaseAndTableWithAlias ident_db_and_name(qualified_asterisk->children[0]);

        for (auto it = tables_expression.begin(); it != tables_expression.end();)
        {
            const ASTTableExpression * table_expression = *it;
            DatabaseAndTableWithAlias database_and_table_with_alias(*table_expression, context.getCurrentDatabase());

            if (ident_db_and_name.satisfies(database_and_table_with_alias, true))
                ++it;
            else
                it = tables_expression.erase(it); /// It's not a required table
        }
    }

    ASTs projection_columns;
    for (auto & table_expression : tables_expression)
    {
        if (table_expression->subquery)
        {
            const auto subquery = static_cast<const ASTSubquery *>(table_expression->subquery.get());
            const auto select_with_union_query = static_cast<ASTSelectWithUnionQuery *>(subquery->children[0].get());
            const auto subquery_projections = getSelectQueryProjectionColumns(select_with_union_query->list_of_selects->children[0]);
            projection_columns.insert(projection_columns.end(), subquery_projections.begin(), subquery_projections.end());
        }
        else
        {
            StoragePtr storage;

            if (table_expression->table_function)
            {
                auto query_context = const_cast<Context *>(&context.getQueryContext());
                storage = query_context->executeTableFunction(table_expression->table_function);
            }
            else if (table_expression->database_and_table_name)
            {
                const auto database_and_table_ast = static_cast<ASTIdentifier*>(table_expression->database_and_table_name.get());
                DatabaseAndTableWithAlias database_and_table_name(*database_and_table_ast);
                storage = context.getTable(database_and_table_name.database, database_and_table_name.table);
            }
            else
                throw Exception("Logical error: unexpected table expression", ErrorCodes::LOGICAL_ERROR);

            const auto block = storage->getSampleBlock();
            for (size_t idx = 0; idx < block.columns(); idx++)
                projection_columns.emplace_back(std::make_shared<ASTIdentifier>(block.getByPosition(idx).name));
        }
    }
    return projection_columns;
}

void PredicateExpressionsOptimizer::cleanExpressionAlias(ASTPtr & expression)
{
    const auto my_alias = expression->tryGetAlias();
    if (!my_alias.empty())
        expression->setAlias("");

    for (auto & child : expression->children)
        cleanExpressionAlias(child);
}

}
