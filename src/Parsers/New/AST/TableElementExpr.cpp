#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TableElementPropertyExpr::TableElementPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr) : property_type(type)
{
    children.push_back(expr);
    (void)property_type; // TODO
}

// static
PtrTo<TableElementExpr> TableElementExpr::createColumn(
    PtrTo<Identifier> name, PtrTo<Identifier> type, PtrTo<TableElementPropertyExpr> property, PtrTo<ColumnExpr> ttl)
{
    return PtrTo<TableElementExpr>(new TableElementExpr(TableElementExpr::ExprType::COLUMN, {name, type, property, ttl}));
}

TableElementExpr::TableElementExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
    (void)expr_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitTableElementColumn(ClickHouseParser::TableElementColumnContext *ctx)
{
    PtrTo<TableElementPropertyExpr> property;
    PtrTo<ColumnExpr> ttl;

    if (ctx->tableElementPropertyExpr()) property = ctx->tableElementPropertyExpr()->accept(this);
    if (ctx->TTL()) ttl = ctx->columnExpr()->accept(this);

    return TableElementExpr::createColumn(ctx->identifier(0)->accept(this), ctx->identifier(1)->accept(this), property, ttl);
}

antlrcpp::Any ParseTreeVisitor::visitTableElementPropertyExpr(ClickHouseParser::TableElementPropertyExprContext *ctx)
{
    TableElementPropertyExpr::PropertyType type;

    if (ctx->DEFAULT()) type = TableElementPropertyExpr::PropertyType::DEFAULT;
    else if (ctx->MATERIALIZED()) type = TableElementPropertyExpr::PropertyType::MATERIALIZED;
    else if (ctx->ALIAS()) type = TableElementPropertyExpr::PropertyType::ALIAS;
    else __builtin_unreachable();

    return std::make_shared<TableElementPropertyExpr>(type, ctx->columnExpr()->accept(this));
}

}
