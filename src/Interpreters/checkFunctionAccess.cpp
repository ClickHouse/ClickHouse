#include <Interpreters/checkFunctionAccess.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/checkStackSize.h>


namespace DB
{

void checkFunctionAccess(const ContextPtr & context, const String & function_name)
{
    if (!AccessControl::hasFunctionsRequiringGrant() || !context)
        return;

    const String registered_name = FunctionFactory::instance().resolveNameOrAlias(function_name);
    if (!AccessControl::functionRequiresGrant(registered_name))
        return;

    context->checkAccess(AccessType::FUNCTION, registered_name);

    /// The query result cache serves a hit before the query is analyzed, so a cached result would skip
    /// the check above after the grant is revoked. Keep the results of such queries out of the cache.
    if (context->hasQueryContext())
        context->getQueryContext()->setCanUseQueryResultCache(false);
}

void checkFunctionAccessInAST(const ContextPtr & context, const ASTPtr & ast)
{
    if (!AccessControl::hasFunctionsRequiringGrant() || !context || !ast)
        return;

    checkStackSize();

    if (const auto * table_expression = ast->as<ASTTableExpression>(); table_expression && table_expression->table_function)
    {
        /// A table function is not a call of the ordinary function with the same name (`file`, `format`), its arguments are.
        for (const auto & child : table_expression->children)
        {
            if (child == table_expression->table_function)
            {
                for (const auto & argument : child->children)
                    checkFunctionAccessInAST(context, argument);
            }
            else
                checkFunctionAccessInAST(context, child);
        }
        return;
    }

    if (const auto * function = ast->as<ASTFunction>())
    {
        if (function->name == "lambda")
        {
            /// `lambda(tuple(x, y), body)`: the parameter list is not a call of `tuple`, check only the body.
            if (function->arguments)
            {
                const auto & lambda_arguments = function->arguments->children;
                for (size_t i = 1; i < lambda_arguments.size(); ++i)
                    checkFunctionAccessInAST(context, lambda_arguments[i]);
            }
            return;
        }

        checkFunctionAccess(context, function->name);
    }

    for (const auto & child : ast->children)
        checkFunctionAccessInAST(context, child);
}

}
