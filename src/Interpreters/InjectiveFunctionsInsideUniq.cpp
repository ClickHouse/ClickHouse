#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InjectiveFunctionsInsideUniq.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

///recursive searching of injective functions
static void removeInjectiveFuctions(ASTPtr & ast, size_t child_number, InjectiveFunctionsInsideUniqMatcher::Data & data)
{
    if (!ast->as<ASTFunction>() || ast->as<ASTFunction>()->arguments->children.size() <= child_number)
        return;

    ASTPtr exact_child = ast->as<ASTFunction>()->arguments->children[child_number];

    const FunctionFactory & function_factory = FunctionFactory::instance();
    const Context & context = data.context;
    if (exact_child->as<ASTFunction>() && function_factory.get(exact_child->as<ASTFunction>()->name, context)->isInjective(Block{}))
    {
        if (exact_child->as<ASTFunction>()->arguments->children.size() == 1)
            ast->as<ASTFunction>()->arguments->children[child_number] = (exact_child->as<ASTFunction>()->arguments->children[0])->clone();
        if (ast->as<ASTFunction>() && exact_child->as<ASTFunction>()->arguments->children.size() == 1)
            killInjectiveFuctions(ast, child_number, data);
    }
}


///cut all injective functions in uniq
void InjectiveFunctionsInsideUniqMatcher::visit(ASTPtr & current_ast, Data data)
{
    auto * function_node = current_ast->as<ASTFunction>();
    if (function_node && function_node->name == "uniq")
    {
        size_t amount_of_children = current_ast->as<ASTFunction>()->arguments->children.size();
        for (size_t i = 0; i < amount_of_children; ++i)
            removeInjectiveFuctions(current_ast, i, data);
    }
}

bool InjectiveFunctionsInsideUniqMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTTableExpression>() || node->as<ASTArrayJoin>())
        return false; // NOLINT
    return true;
}

}
