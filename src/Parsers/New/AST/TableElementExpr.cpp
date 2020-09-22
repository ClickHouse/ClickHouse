#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/ColumnTypeExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TableColumnPropertyExpr::TableColumnPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr) : property_type(type)
{
    children.push_back(expr);
}

ASTPtr TableColumnPropertyExpr::convertToOld() const
{
    return children[EXPR]->convertToOld();
}

// static
PtrTo<TableElementExpr> TableElementExpr::createColumn(
    PtrTo<Identifier> name,
    PtrTo<ColumnTypeExpr> type,
    PtrTo<TableColumnPropertyExpr> property,
    PtrTo<StringLiteral> comment,
    PtrTo<ColumnExpr> ttl)
{
    return PtrTo<TableElementExpr>(new TableElementExpr(TableElementExpr::ExprType::COLUMN, {name, type, property, comment, ttl}));
}

TableElementExpr::TableElementExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr TableElementExpr::convertToOld() const
{
    switch(expr_type)
    {
        case ExprType::COLUMN:
        {
            auto expr = std::make_shared<ASTColumnDeclaration>();

            expr->name = children[NAME]->as<Identifier>()->getName(); // FIXME: do we have correct nested identifier here already?
            if (has(TYPE))
            {
                expr->type = children[TYPE]->convertToOld();
                expr->children.push_back(expr->type);
            }
            if (has(PROPERTY))
            {
                switch(children[PROPERTY]->as<TableColumnPropertyExpr>()->getType())
                {
                    case TableColumnPropertyExpr::PropertyType::ALIAS:
                        expr->default_specifier = "ALIAS";
                        break;
                    case TableColumnPropertyExpr::PropertyType::DEFAULT:
                        expr->default_specifier = "DEFAULT";
                        break;
                    case TableColumnPropertyExpr::PropertyType::MATERIALIZED:
                        expr->default_specifier = "MATERIALIZED";
                        break;
                }
                expr->default_expression = children[PROPERTY]->convertToOld();
                expr->children.push_back(expr->default_expression);
            }
            if (has(COMMENT))
            {
                expr->comment = children[COMMENT]->convertToOld();
                expr->children.push_back(expr->comment);
            }
            // TODO: CODEC
            if (has(TTL))
            {
                expr->ttl = children[TTL]->convertToOld();
                expr->children.push_back(expr->ttl);
            }

            return expr;
        }
        case ExprType::CONSTRAINT:
        {
            auto expr = std::make_shared<ASTConstraintDeclaration>();

            // TODO

            return expr;
        }
        case ExprType::INDEX:
        {
            auto expr = std::make_shared<ASTIndexDeclaration>();

            // TODO

            return expr;
        }
    }
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

    return std::make_shared<TableColumnPropertyExpr>(type, visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitTableElementExprColumn(ClickHouseParser::TableElementExprColumnContext *ctx)
{
    if (ctx->tableColumnDfnt())
        return visit(ctx->tableColumnDfnt());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *ctx)
{
    PtrTo<TableColumnPropertyExpr> property;
    PtrTo<ColumnTypeExpr> type;
    PtrTo<StringLiteral> comment;
    PtrTo<ColumnExpr> ttl;

    if (ctx->tableColumnPropertyExpr()) property = visit(ctx->tableColumnPropertyExpr());
    if (ctx->columnTypeExpr()) type = visit(ctx->columnTypeExpr());
    if (ctx->STRING_LITERAL()) comment = Literal::createString(ctx->STRING_LITERAL());
    if (ctx->TTL()) ttl = visit(ctx->columnExpr());

    return TableElementExpr::createColumn(visit(ctx->nestedIdentifier()), type, property, comment, ttl);
}

}
