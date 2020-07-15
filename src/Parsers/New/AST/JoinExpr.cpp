#include <Parsers/New/AST/JoinExpr.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<JoinExpr> JoinExpr::createTableExpr(PtrTo<TableExpr> expr)
{
    return PtrTo<JoinExpr>(new JoinExpr(JoinExpr::ExprType::TABLE, {expr}));
}

JoinExpr::JoinExpr(JoinExpr::ExprType type, std::list<Ptr> exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr JoinExpr::convertToOld() const
{
    auto list = std::make_shared<ASTExpressionList>();

    if (expr_type == ExprType::TABLE) list->children.emplace_back(children[0]->convertToOld());
    else if (expr_type == ExprType::JOIN_OP)
    {
        auto left = children[0]->convertToOld(), right = children[1]->convertToOld();
        list->children.insert(list->children.end(), left->children.begin(), left->children.end());
        list->children.insert(list->children.end(), right->children.begin(), right->children.end());
    }

    return list;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitJoinExprTable(ClickHouseParser::JoinExprTableContext *ctx)
{
    return AST::JoinExpr::createTableExpr(ctx->tableExpr()->accept(this));
}

}
