#include <Common/typeid_cast.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/RemoveInjectiveFunctionsVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/getASTFunctionArgumentColumns.h>

namespace DB
{

static bool isUniq(const ASTFunction & func)
{
    return func.name == "uniq" || func.name == "uniqExact" || func.name == "uniqHLL12"
        || func.name == "uniqCombined" || func.name == "uniqCombined64"
        || func.name == "uniqTheta";
}

/// Remove injective functions of one argument: replace with a child
static bool removeInjectiveFunction(
    ASTPtr & ast, ContextPtr context, const NamesAndTypesList & source_columns, const FunctionFactory & function_factory)
{
    const ASTFunction * func = ast->as<ASTFunction>();
    if (!func)
        return false;

    if (!func->arguments || func->arguments->children.size() != 1)
        return false;

    /// The claim can depend on the argument, so resolve it as far as the AST allows. An argument
    /// that stays unresolved leaves the function unclaimed.
    auto argument_columns = tryGetASTFunctionArgumentColumns(*func, source_columns);
    if (!argument_columns || !function_factory.get(func->name, context)->isInjective(*argument_columns))
        return false;

    ast = func->arguments->children[0];
    return true;
}

void RemoveInjectiveFunctionsMatcher::visit(ASTPtr & ast, const Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, ast, data);
}

void RemoveInjectiveFunctionsMatcher::visit(ASTFunction & func, ASTPtr &, const Data & data)
{
    if (isUniq(func))
    {
        const FunctionFactory & function_factory = FunctionFactory::instance();

        for (auto & arg : func.arguments->children)
        {
            while (removeInjectiveFunction(arg, data.getContext(), data.source_columns, function_factory))
                ;
        }
    }
}

bool RemoveInjectiveFunctionsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    if (node->as<ASTSubquery>() ||
        node->as<ASTTableExpression>())
        return false; // NOLINT
    return true;
}

}
