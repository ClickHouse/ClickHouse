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
        const auto & tuple = literal->value.safeGet<const Tuple &>();
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

class SplitTupleComparsionExpressionMatcher
{
public:
    using Data = ComparisonTupleEliminationMatcher::Data;

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
    static void visit(ASTPtr & ast, Data &)
    {
        auto * func = ast->as<ASTFunction>();
        if (!func || func->arguments->children.size() != 2)
            return;

        if (func->name != "equals" && func->name != "notEquals")
            return;

        auto lhs = splitTuple(func->arguments->children[0]);
        auto rhs = splitTuple(func->arguments->children[1]);
        if (lhs.size() != rhs.size() || lhs.empty())
            return;

        ASTs new_args;
        new_args.reserve(lhs.size());
        for (size_t i = 0; i < lhs.size(); ++i)
        {
            new_args.emplace_back(makeASTFunction("equals", lhs[i], rhs[i]));
        }

        if (func->name == "notEquals")
            ast = makeASTFunction("not", concatWithAnd(new_args));
        else
            ast = concatWithAnd(new_args);
    }
};

using SplitTupleComparsionExpressionVisitor = InDepthNodeVisitor<SplitTupleComparsionExpressionMatcher, true>;

}

bool ComparisonTupleEliminationMatcher::needChildVisit(ASTPtr &, const ASTPtr &)
{
    return true;
}

void ComparisonTupleEliminationMatcher::visit(ASTPtr & ast, Data & data)
{
    auto * select_ast = ast->as<ASTSelectQuery>();
    if (!select_ast || !select_ast->where())
        return;

    if (select_ast->where())
        SplitTupleComparsionExpressionVisitor(data).visit(select_ast->refWhere());
}

}
