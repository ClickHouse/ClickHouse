#include <Parsers/New/AST/TableExpr.h>
#include <support/Any.h>
#include "Parsers/New/AST/Identifier.h"
#include "Parsers/New/ParseTreeVisitor.h"


namespace DB::AST
{

// static
PtrTo<TableExpr> TableExpr::createIdentifier(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<TableExpr>(new TableExpr(TableExpr::ExprType::IDENTIFIER, {identifier}));
}

TableExpr::TableExpr(TableExpr::ExprType type, std::list<Ptr> exprs) : expr_type(type)
{
    children = exprs;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx)
{
    return AST::TableExpr::createIdentifier(ctx->tableIdentifier()->accept(this).as<AST::PtrTo<AST::TableIdentifier>>());
}

}
