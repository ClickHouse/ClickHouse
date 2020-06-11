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
    constexpr const char * any = "any";
}

ASTPtr * getExactChild(const ASTPtr & ast, const size_t ind)
{
    return &ast->as<ASTFunction>()->arguments->children[ind];
}

///recursive searching of identifiers
void changeAllIdentifiers(ASTPtr & ast, size_t ind)
{
    ASTPtr * exact_child = getExactChild(ast, ind);
    ast->dumpTree(std::cerr);
    if ((*exact_child)->as<ASTIdentifier>())
    {
        ///put new any
        ASTPtr oldAST = *exact_child;
        *exact_child = makeASTFunction(any);
        (*exact_child)->as<ASTFunction>()->arguments->children.push_back(oldAST);
    }
    else if ((*exact_child)->as<ASTFunction>() &&
        !AggregateFunctionFactory::instance().isAggregateFunctionName((*exact_child)->as<ASTFunction>()->name))
        for (size_t i = 0; i < (*exact_child)->as<ASTFunction>()->arguments->children.size(); i++)
            changeAllIdentifiers(*exact_child, i);
    else if (ast->as<ASTFunction>()->arguments->children[ind]->as<ASTFunction>() &&
             AggregateFunctionFactory::instance().isAggregateFunctionName((*exact_child)->as<ASTFunction>()->name))
        throw Exception("Aggregate function " + (*exact_child)->as<ASTFunction>()->name +
                            " is found inside aggregate function any in query", ErrorCodes::ILLEGAL_AGGREGATION);
}


///cut old any, put any to identifiers. any(functions(x)) -> functions(any(x1))
void AnyInputMatcher::visit(ASTPtr & current_ast, Data data)
{
    data = {};
    if (!current_ast)
        return;

    auto * function_node = current_ast->as<ASTFunction>();
    if (function_node && function_node->name == any && function_node->arguments->children[0]->as<ASTFunction>())
    {
        ///cut any
        current_ast = (function_node->arguments->children[0])->clone();
        for (size_t i = 0; i < current_ast->as<ASTFunction>()->arguments->children.size(); ++i)
            changeAllIdentifiers(current_ast, i);
    }
}

bool AnyInputMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (!child)
        throw Exception("AST item should not have nullptr in children", ErrorCodes::LOGICAL_ERROR);

    if (node->as<ASTTableExpression>() || node->as<ASTArrayJoin>())
        return false;

    return true;
}

}
