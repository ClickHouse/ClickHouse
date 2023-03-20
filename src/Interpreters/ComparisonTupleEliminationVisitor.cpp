#include <Interpreters/ComparisonTupleEliminationVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{


ASTs splitTuple(const ASTPtr & node)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "tuple")
        return func->arguments->children;

    if (const auto * literal = node->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::Tuple)
    {
        ASTs result;
        const auto & tuple = literal->value.get<const Tuple &>();
        for (const auto & child : tuple)
            result.emplace_back(std::make_shared<ASTLiteral>(child));
        return result;
    }

    return {};
}


ASTPtr concatWithAnd(const ASTs & nodes)
{
    if (nodes.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot concat empty list of nodes");

    if (nodes.size() == 1)
        return nodes[0];

    auto result = makeASTFunction("and");
    result->arguments->children = nodes;
    return result;
}

void trySplitTupleComparsion(ASTPtr & expression)
{
    if (!expression)
        return;

    auto * func = expression->as<ASTFunction>();
    if (!func)
        return;

    if (func->name == "and" || func->name == "or" || func->name == "not" || func->name == "tuple")
    {
        for (auto & child : func->arguments->children)
        {
            trySplitTupleComparsion(child);
        }
    }

    if (func->name == "equals" || func->name == "notEquals")
    {
        if (func->arguments->children.size() != 2)
            return;

        auto lhs = splitTuple(func->arguments->children[0]);
        auto rhs = splitTuple(func->arguments->children[1]);
        if (lhs.size() != rhs.size() || lhs.empty() || rhs.empty())
            return;

        ASTs new_args;
        for (size_t i = 0; i < lhs.size(); ++i)
        {
            trySplitTupleComparsion(lhs[i]);
            trySplitTupleComparsion(rhs[i]);
            new_args.emplace_back(makeASTFunction("equals", lhs[i], rhs[i]));
        }

        if (func->name == "notEquals")
            expression = makeASTFunction("not", concatWithAnd(new_args));
        else
            expression = concatWithAnd(new_args);
    }
}

}

bool ComparisonTupleEliminationMatcher::needChildVisit(ASTPtr &, const ASTPtr &)
{
    return true;
}

void ComparisonTupleEliminationMatcher::visit(ASTPtr & ast, Data &)
{
    auto * select_ast = ast->as<ASTSelectQuery>();
    if (!select_ast || !select_ast->where())
        return;

    if (select_ast->where())
        trySplitTupleComparsion(select_ast->refWhere());
}

}
