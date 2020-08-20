#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/ColumnTypeExpr.h>
#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TableColumnPropertyExpr::TableColumnPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr) : property_type(type)
{
    children.push_back(expr);
    (void)property_type; // TODO
}

// static
PtrTo<TableElementExpr> TableElementExpr::createColumn(
    PtrTo<Identifier> name, PtrTo<ColumnTypeExpr> type, PtrTo<TableColumnPropertyExpr> property, PtrTo<ColumnExpr> ttl)
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

antlrcpp::Any ParseTreeVisitor::visitTableColumnPropertyExpr(ClickHouseParser::TableColumnPropertyExprContext *ctx)
{
    TableColumnPropertyExpr::PropertyType type;

    if (ctx->DEFAULT()) type = TableColumnPropertyExpr::PropertyType::DEFAULT;
    else if (ctx->MATERIALIZED()) type = TableColumnPropertyExpr::PropertyType::MATERIALIZED;
    else if (ctx->ALIAS()) type = TableColumnPropertyExpr::PropertyType::ALIAS;
    else __builtin_unreachable();

    return std::make_shared<TableColumnPropertyExpr>(type, ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitTableElementExprColumn(ClickHouseParser::TableElementExprColumnContext *ctx)
{
    if (ctx->tableColumnDfnt())
        return ctx->tableColumnDfnt()->accept(this);
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *ctx)
{
    PtrTo<TableColumnPropertyExpr> property;
    PtrTo<ColumnTypeExpr> type;
    PtrTo<ColumnExpr> ttl;

    if (ctx->tableColumnPropertyExpr()) property = ctx->tableColumnPropertyExpr()->accept(this);
    if (ctx->columnTypeExpr()) type = ctx->columnTypeExpr()->accept(this);
    if (ctx->TTL()) ttl = ctx->columnExpr()->accept(this);

    return TableElementExpr::createColumn(ctx->identifier()->accept(this), type, property, ttl);
}

}
