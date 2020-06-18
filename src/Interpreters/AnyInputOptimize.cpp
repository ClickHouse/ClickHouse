#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/AnyInputOptimize.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_AGGREGATION;
}

namespace
{
    constexpr auto * any = "any";
    constexpr auto * anyLast = "anyLast";
}

ASTPtr * getExactChild(const ASTPtr & ast, const size_t ind)
{
    if (ast && ast->as<ASTFunction>()->arguments->children[ind])
        return &ast->as<ASTFunction>()->arguments->children[ind];
    return nullptr;
}

///recursive searching of identifiers
void changeAllIdentifiers(ASTPtr & ast, size_t ind, const std::string & name)
{
    ASTPtr * exact_child = getExactChild(ast, ind);
    if (!exact_child)
        return;

    if ((*exact_child)->as<ASTIdentifier>())
    {
        ///put new any
        ASTPtr old_ast = *exact_child;
        *exact_child = makeASTFunction(name);
        (*exact_child)->as<ASTFunction>()->arguments->children.push_back(old_ast);
    }
    else if ((*exact_child)->as<ASTFunction>())
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName((*exact_child)->as<ASTFunction>()->name))
            throw Exception("Aggregate function " + (*exact_child)->as<ASTFunction>()->name +
                " is found inside aggregate function " + name + " in query", ErrorCodes::ILLEGAL_AGGREGATION);

        for (size_t i = 0; i < (*exact_child)->as<ASTFunction>()->arguments->children.size(); i++)
            changeAllIdentifiers(*exact_child, i, name);
    }
}


///cut old any, put any to identifiers. any(functions(x)) -> functions(any(x))
void AnyInputMatcher::visit(ASTPtr & current_ast, Data data)
{
    data = {};
    if (!current_ast)
        return;

    auto * function_node = current_ast->as<ASTFunction>();
    if (!function_node || function_node->arguments->children.empty())
        return;

    const auto & function_argument = function_node->arguments->children[0];
    if ((function_node->name == any || function_node->name == anyLast)
        && function_argument && function_argument->as<ASTFunction>())
    {
        auto name = function_node->name;
        ///cut any or anyLast
        if (!function_argument->as<ASTFunction>()->arguments->children.empty())
        {
            current_ast = function_argument->clone();
            for (size_t i = 0; i < current_ast->as<ASTFunction>()->arguments->children.size(); ++i)
                changeAllIdentifiers(current_ast, i, name);
        }
    }
}

bool AnyInputMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (!child)
        throw Exception("AST item should not have nullptr in children", ErrorCodes::LOGICAL_ERROR);

    if (node->as<ASTTableExpression>() || node->as<ASTArrayJoin>())
        return false; // NOLINT

    return true;
}

}
