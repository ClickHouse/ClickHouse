#include <Interpreters/checkFunctionAccess.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>


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

void checkFunctionAccessInExpression(const ContextPtr & context, const ASTPtr & ast)
{
    if (!AccessControl::hasFunctionsRequiringGrant() || !context || !ast)
        return;

    if (const auto * function = ast->as<ASTFunction>(); function && function->name != "lambda")
        checkFunctionAccess(context, function->name);

    for (const auto & child : ast->children)
        checkFunctionAccessInExpression(context, child);
}

}
