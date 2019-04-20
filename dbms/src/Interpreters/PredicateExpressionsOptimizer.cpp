#include <iostream>

#include <Common/typeid_cast.h>
#include <Storages/IStorage.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/FindIdentifierBestTableVisitor.h>
#include <Interpreters/ExtractFunctionDataVisitor.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}

namespace
{

constexpr auto and_function_name = "and";

String qualifiedName(ASTIdentifier * identifier, const String & prefix)
{
    if (identifier->isShort())
        return prefix + identifier->getAliasOrColumnName();
    return identifier->getAliasOrColumnName();
}

} // namespace

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    ASTSelectQuery * ast_select_, ExtractedSettings && settings_, const Context & context_)
    : ast_select(ast_select_), settings(settings_), context(context_)
{
}

bool PredicateExpressionsOptimizer::optimize()
{
    if (!settings.enable_optimize_predicate_expression || !ast_select || !ast_select->tables() || ast_select->tables()->children.empty())
        return false;

    if (!ast_select->where() && !ast_select->prewhere())
        return false;

    if (ast_select->array_join_expression_list())
        return false;

    SubqueriesProjectionColumns all_subquery_projection_columns = getAllSubqueryProjectionColumns();

    bool is_rewrite_subqueries = false;
    if (!all_subquery_projection_columns.empty())
    {
        is_rewrite_subqueries |= optimizeImpl(ast_select->where(), all_subquery_projection_columns, OptimizeKind::PUSH_TO_WHERE);
        is_rewrite_subqueries |= optimizeImpl(ast_select->prewhere(), all_subquery_projection_columns, OptimizeKind::PUSH_TO_PREWHERE);
    }

    return is_rewrite_subqueries;
}

bool PredicateExpressionsOptimizer::optimizeImpl(
    const ASTPtr & outer_expression, const SubqueriesProjectionColumns & subqueries_projection_columns, OptimizeKind expression_kind)
{
    /// split predicate with `and`
    std::vector<ASTPtr> outer_predicate_expressions = splitConjunctionPredicate(outer_expression);

    std::vector<TableWithColumnNames> tables_with_columns = getDatabaseAndTablesWithColumnNames(*ast_select, context);

    bool is_rewrite_subquery = false;
    for (auto & outer_predicate : outer_predicate_expressions)
    {
        if (isArrayJoinFunction(outer_predicate))
            continue;

        auto outer_predicate_dependencies = getDependenciesAndQualifiers(outer_predicate, tables_with_columns);

        /// TODO: remove origin expression
        for (const auto & [subquery, projection_columns] : subqueries_projection_columns)
        {
            OptimizeKind optimize_kind = OptimizeKind::NONE;
            if (allowPushDown(subquery, outer_predicate, projection_columns, outer_predicate_dependencies, optimize_kind))
            {
                if (optimize_kind == OptimizeKind::NONE)
                    optimize_kind = expression_kind;

                ASTPtr inner_predicate = outer_predicate->clone();
                cleanExpressionAlias(inner_predicate); /// clears the alias name contained in the outer predicate

                std::vector<IdentifierWithQualifier> inner_predicate_dependencies =
                    getDependenciesAndQualifiers(inner_predicate, tables_with_columns);

                setNewAliasesForInnerPredicate(projection_columns, inner_predicate_dependencies);

                switch (optimize_kind)
                {
                    case OptimizeKind::NONE: continue;
                    case OptimizeKind::PUSH_TO_WHERE:
                        is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery, ASTSelectQuery::Expression::WHERE);
                        continue;
                    case OptimizeKind::PUSH_TO_HAVING:
                        is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery, ASTSelectQuery::Expression::HAVING);
                        continue;
                    case OptimizeKind::PUSH_TO_PREWHERE:
                        is_rewrite_subquery |= optimizeExpression(inner_predicate, subquery, ASTSelectQuery::Expression::PREWHERE);
                        continue;
                }
            }
        }
    }
    return is_rewrite_subquery;
}

bool PredicateExpressionsOptimizer::allowPushDown(
    const ASTSelectQuery * subquery,
    const ASTPtr &,
    const std::vector<ProjectionWithAlias> & projection_columns,
    const std::vector<IdentifierWithQualifier> & dependencies,
    OptimizeKind & optimize_kind)
{
    if (!subquery || subquery->final() || subquery->limitBy() || subquery->limitLength() || subquery->with())
        return false;
    else
    {
        ASTPtr expr_list = ast_select->select();
        ExtractFunctionVisitor::Data extract_data;
        ExtractFunctionVisitor(extract_data).visit(expr_list);

        for (const auto & subquery_function : extract_data.functions)
        {
            const auto & function = FunctionFactory::instance().tryGet(subquery_function->name, context);

            /// Skip lambda, tuple and other special functions
            if (function && function->isStateful())
                return false;
        }
    }

    const auto * ast_join = ast_select->join();
    const ASTTableExpression * left_table_expr = nullptr;
    const ASTTableExpression * right_table_expr = nullptr;
    const ASTSelectQuery * left_subquery = nullptr;
    const ASTSelectQuery * right_subquery = nullptr;

    if (ast_join)
    {
        left_table_expr = ast_select
                                ->tables()->as<ASTTablesInSelectQuery>()
                                ->children[0]->as<ASTTablesInSelectQueryElement>()
                                ->table_expression->as<ASTTableExpression>();
        right_table_expr = ast_select
                                ->tables()->as<ASTTablesInSelectQuery>()
                                ->children[1]->as<ASTTablesInSelectQueryElement>()
                                ->table_expression->as<ASTTableExpression>();

        if (left_table_expr && left_table_expr->subquery)
            left_subquery = left_table_expr->subquery
                                ->children[0]->as<ASTSelectWithUnionQuery>()
                                ->list_of_selects->children[0]->as<ASTSelectQuery>();
        if (right_table_expr && right_table_expr->subquery)
            right_subquery = right_table_expr->subquery
                                ->children[0]->as<ASTSelectWithUnionQuery>()
                                ->list_of_selects->children[0]->as<ASTSelectQuery>();

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

        /// Check right side for LEFT'o'FULL JOIN
        if (isLeftOrFull(ast_join->table_join->as<ASTTableJoin>()->kind) && right_subquery == subquery)
            return false;

        /// Check left side for RIGHT'o'FULL JOIN
        if (isRightOrFull(ast_join->table_join->as<ASTTableJoin>()->kind) && left_subquery == subquery)
            return false;
    }

    return checkDependencies(projection_columns, dependencies, optimize_kind);
}

bool PredicateExpressionsOptimizer::checkDependencies(
    const std::vector<ProjectionWithAlias> & projection_columns,
    const std::vector<IdentifierWithQualifier> & dependencies,
    OptimizeKind & optimize_kind)
{
    for (const auto & [identifier, prefix] : dependencies)
    {
        bool is_found = false;
        String qualified_name = qualifiedName(identifier, prefix);

        for (const auto & [ast, alias] : projection_columns)
        {
            if (alias == qualified_name)
            {
                is_found = true;
                ASTPtr projection_column = ast;
                ExtractFunctionVisitor::Data extract_data;
                ExtractFunctionVisitor(extract_data).visit(projection_column);

                if (!extract_data.aggregate_functions.empty())
                    optimize_kind = OptimizeKind::PUSH_TO_HAVING;
            }
        }

        if (!is_found)
            return false;
    }

    return true;
}

std::vector<ASTPtr> PredicateExpressionsOptimizer::splitConjunctionPredicate(const ASTPtr & predicate_expression)
{
    std::vector<ASTPtr> predicate_expressions;

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

            if (const auto * function = expression->as<ASTFunction>())
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

std::vector<PredicateExpressionsOptimizer::IdentifierWithQualifier>
PredicateExpressionsOptimizer::getDependenciesAndQualifiers(ASTPtr & expression, std::vector<TableWithColumnNames> & tables)
{
    FindIdentifierBestTableVisitor::Data find_data(tables);
    FindIdentifierBestTableVisitor(find_data).visit(expression);

    std::vector<IdentifierWithQualifier> dependencies;

    for (const auto & [identifier, table] : find_data.identifier_table)
    {
        String table_alias;
        if (table)
            table_alias = table->getQualifiedNamePrefix();

        dependencies.emplace_back(identifier, table_alias);
    }

    return dependencies;
}

void PredicateExpressionsOptimizer::setNewAliasesForInnerPredicate(
    const std::vector<ProjectionWithAlias> & projection_columns,
    const std::vector<IdentifierWithQualifier> & dependencies)
{
    for (auto & [identifier, prefix] : dependencies)
    {
        String qualified_name = qualifiedName(identifier, prefix);

        for (auto & [ast, alias] : projection_columns)
        {
            if (alias == qualified_name)
            {
                String name;
                if (auto * id = ast->as<ASTIdentifier>())
                {
                    name = id->tryGetAlias();
                    if (name.empty())
                        name = id->shortName();
                }
                else
                {
                    if (ast->tryGetAlias().empty())
                        ast->setAlias(ast->getColumnName());
                    name = ast->getAliasOrColumnName();
                }

                identifier->setShortName(name);
            }
        }
    }
}

bool PredicateExpressionsOptimizer::isArrayJoinFunction(const ASTPtr & node)
{
    if (const auto * function = node->as<ASTFunction>())
    {
        if (function->name == "arrayJoin")
            return true;
    }

    for (auto & child : node->children)
        if (isArrayJoinFunction(child))
            return true;

    return false;
}

bool PredicateExpressionsOptimizer::optimizeExpression(const ASTPtr & outer_expression, ASTSelectQuery * subquery,
                                                       ASTSelectQuery::Expression expr)
{
    ASTPtr subquery_expression = subquery->getExpression(expr, false);
    subquery_expression = subquery_expression ? makeASTFunction(and_function_name, outer_expression, subquery_expression) : outer_expression;

    subquery->setExpression(expr, std::move(subquery_expression));
    return true;
}

PredicateExpressionsOptimizer::SubqueriesProjectionColumns PredicateExpressionsOptimizer::getAllSubqueryProjectionColumns()
{
    SubqueriesProjectionColumns projection_columns;

    for (const auto & table_expression : getSelectTablesExpression(*ast_select))
        if (table_expression->subquery)
            getSubqueryProjectionColumns(table_expression->subquery, projection_columns);

    return projection_columns;
}

void PredicateExpressionsOptimizer::getSubqueryProjectionColumns(const ASTPtr & subquery, SubqueriesProjectionColumns & projection_columns)
{
    String qualified_name_prefix = subquery->tryGetAlias();
    if (!qualified_name_prefix.empty())
        qualified_name_prefix += '.';

    const ASTPtr & subselect = subquery->children[0];

    ASTs select_with_union_projections;
    const auto * select_with_union_query = subselect->as<ASTSelectWithUnionQuery>();

    for (auto & select : select_with_union_query->list_of_selects->children)
    {
        std::vector<ProjectionWithAlias> subquery_projections;
        auto select_projection_columns = getSelectQueryProjectionColumns(select);

        if (!select_projection_columns.empty())
        {
            if (select_with_union_projections.empty())
                select_with_union_projections = select_projection_columns;

            for (size_t i = 0; i < select_projection_columns.size(); i++)
                subquery_projections.emplace_back(std::pair(select_projection_columns[i],
                                                            qualified_name_prefix + select_with_union_projections[i]->getAliasOrColumnName()));

            projection_columns.insert(std::pair(select->as<ASTSelectQuery>(), subquery_projections));
        }
    }
}

ASTs PredicateExpressionsOptimizer::getSelectQueryProjectionColumns(ASTPtr & ast)
{
    ASTs projection_columns;
    auto * select_query = ast->as<ASTSelectQuery>();

    /// first should normalize query tree.
    std::unordered_map<String, ASTPtr> aliases;
    std::vector<DatabaseAndTableWithAlias> tables = getDatabaseAndTables(*select_query, context.getCurrentDatabase());

    /// TODO: get tables from evaluateAsterisk instead of tablesOnly() to extract asterisks in general way
    std::vector<TableWithColumnNames> tables_with_columns = TranslateQualifiedNamesVisitor::Data::tablesOnly(tables);
    TranslateQualifiedNamesVisitor::Data qn_visitor_data({}, tables_with_columns, false);
    TranslateQualifiedNamesVisitor(qn_visitor_data).visit(ast);

    QueryAliasesVisitor::Data query_aliases_data{aliases};
    QueryAliasesVisitor(query_aliases_data).visit(ast);

    QueryNormalizer::Data normalizer_data(aliases, settings);
    QueryNormalizer(normalizer_data).visit(ast);

    for (const auto & projection_column : select_query->select()->children)
    {
        if (projection_column->as<ASTAsterisk>() || projection_column->as<ASTQualifiedAsterisk>())
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
    if (!select_query->tables() || select_query->tables()->children.empty())
        return {};

    std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(*select_query);

    if (const auto * qualified_asterisk = asterisk->as<ASTQualifiedAsterisk>())
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
            const auto * subquery = table_expression->subquery->as<ASTSubquery>();
            const auto * select_with_union_query = subquery->children[0]->as<ASTSelectWithUnionQuery>();
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
                const auto * database_and_table_ast = table_expression->database_and_table_name->as<ASTIdentifier>();
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
