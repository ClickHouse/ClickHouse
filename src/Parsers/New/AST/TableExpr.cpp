#include <Parsers/New/AST/TableExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB::AST
{

// static
PtrTo<TableExpr> TableExpr::createIdentifier(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<TableExpr>(new TableExpr(TableExpr::ExprType::IDENTIFIER, {identifier}));
}

TableExpr::TableExpr(TableExpr::ExprType type, std::vector<Ptr> exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr TableExpr::convertToOld() const
{
    auto expr = std::make_shared<ASTTableExpression>();

    // TODO: SAMPLE and RATIO also goes here somehow

    switch (expr_type)
    {
        case ExprType::IDENTIFIER:
            expr->database_and_table_name = children[IDENTIFIER]->convertToOld();
            expr->children.emplace_back(expr->database_and_table_name);
            break;
        default:
            throw std::logic_error("Table expressions other than Identifier are not supported for now");
    }

    return expr;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx)
{
    return AST::TableExpr::createIdentifier(ctx->tableIdentifier()->accept(this).as<AST::PtrTo<AST::TableIdentifier>>());
}

}
