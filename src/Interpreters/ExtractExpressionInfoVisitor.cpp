#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>


namespace DB
{

void ExpressionInfoMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, ast, data);
    else if (const auto * identifier = ast->as<ASTIdentifier>())
        visit(*identifier, ast, data);
}

void ExpressionInfoMatcher::visit(const ASTFunction & ast_function, const ASTPtr &, Data & data)
{
    if (ast_function.name == "arrayJoin")
    {
        data.is_array_join = true;
    }
    else if (ast_function.name == "untuple")
    {
        data.is_untuple = true;
    }
    // "is_aggregate_function" is used to determine whether we can move a filter
    // (1) from HAVING to WHERE or (2) from WHERE of a parent query to HAVING of
    // a subquery.
    // For aggregate functions we can't do (1) but can do (2).
    // For window functions both don't make sense -- they are not allowed in
    // WHERE or HAVING.
    else if (!ast_function.isWindowFunction()
        && AggregateFunctionFactory::instance().isAggregateFunctionName(
            ast_function.name))
    {
        data.is_aggregate_function = true;
    }
    else if (ast_function.isWindowFunction())
    {
        data.is_window_function = true;
    }
    else
    {
        /// User-defined functions are not registered in `FunctionFactory`, so they have to be resolved
        /// through their own factories, the same way `ActionsVisitor` and `TreeOptimizer` do. Otherwise
        /// a non-deterministic `EXECUTABLE` or `WASM` UDF looks like an unknown function here and is
        /// treated as deterministic, so a predicate that calls it may be duplicated into a subquery and
        /// evaluated twice per row.
        ///
        /// `EXECUTABLE` UDFs are never deterministic in the scope of a query, so their determinism is
        /// decided by the name alone. Do not instantiate them: `UserDefinedExecutableFunctionFactory::tryGet`
        /// builds a `UserDefinedFunction` with an empty `parameters` array, and that constructor throws
        /// `BAD_ARGUMENTS` for a parametric UDF such as `test_function_with_parameter(1)(k)`, which would
        /// turn a mere optimizer walk into a query failure.
        if (UserDefinedExecutableFunctionFactory::has(ast_function.name, data.getContext()))
        {
            data.is_deterministic_function = false;
            return;
        }

        FunctionOverloadResolverPtr function;

        {
            auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(ast_function.name);
            if (user_defined_function && user_defined_function->as<ASTCreateWasmFunctionQuery>())
            {
                UserDefinedWebAssemblyFunctionFactory::checkWebAssemblyIsAvailable(data.getContext());
                function = UserDefinedWebAssemblyFunctionFactory::instance().tryGet(ast_function.name, data.getContext());
            }
        }

        if (!function)
            function = FunctionFactory::instance().tryGet(ast_function.name, data.getContext());

        /// Skip lambda, tuple and other special functions
        if (function)
        {
            if (function->isStateful())
                data.is_stateful_function = true;

            if (!function->isDeterministicInScopeOfQuery())
                data.is_deterministic_function = false;
        }
    }
}

void ExpressionInfoMatcher::visit(const ASTIdentifier & identifier, const ASTPtr &, Data & data)
{
    if (!identifier.compound())
    {
        for (size_t index = 0; index < data.tables.size(); ++index)
        {
            const auto & table = data.tables[index];

            // TODO: make sure no collision ever happens
            if (table.hasColumn(identifier.name()))
            {
                data.unique_reference_tables_pos.emplace(index);
                break;
            }
        }
    }
    else
    {
        if (auto best_table_pos = IdentifierSemantic::chooseTable(identifier, data.tables))
            data.unique_reference_tables_pos.emplace(*best_table_pos);
    }
}

bool ExpressionInfoMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>();
}

bool hasNonRewritableFunction(const ASTPtr & node, ContextPtr context)
{
    for (const auto & select_expression : node->children)
    {
        TablesWithColumns tables;
        ExpressionInfoVisitor::Data expression_info{WithContext{context}, tables};
        ExpressionInfoVisitor(expression_info).visit(select_expression);

        /// `untuple` expands at execution build time: its output names are not referenceable here.
        if (expression_info.is_stateful_function
            || expression_info.is_window_function
            || expression_info.is_untuple)
        {
            // If an outer query has a WHERE on window function, we can't move
            // it into the subquery, because window functions are not allowed in
            // WHERE and HAVING. Example:
            // select * from (
            //     select number,
            //          count(*) over (partition by intDiv(number, 3)) c
            //     from numbers(3)
            // ) where c > 1;
            return true;
        }
    }

    return false;
}

}

