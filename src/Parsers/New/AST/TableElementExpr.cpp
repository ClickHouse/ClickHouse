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

CodecArgExpr::CodecArgExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> list) : INode{identifier, list}
{
}

ASTPtr CodecArgExpr::convertToOld() const
{
    auto func = std::make_shared<ASTFunction>();

    func->name = get<Identifier>(NAME)->getName();
    if (has(ARGS))
    {
        func->arguments = get(ARGS)->convertToOld();
        func->children.push_back(func->arguments);
    }

    return func;
}

CodecExpr::CodecExpr(PtrTo<CodecArgList> list) : INode{list}
{
}

ASTPtr CodecExpr::convertToOld() const
{
    auto func = std::make_shared<ASTFunction>();

    func->name = "codec";
    func->arguments = get(ARGS)->convertToOld();
    func->children.push_back(func->arguments);

    return func;
}

TableColumnPropertyExpr::TableColumnPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr) : INode{expr}, property_type(type)
{
}

ASTPtr TableColumnPropertyExpr::convertToOld() const
{
    return get(EXPR)->convertToOld();
}

// static
PtrTo<TableElementExpr> TableElementExpr::createColumn(
    PtrTo<Identifier> name,
    PtrTo<ColumnTypeExpr> type,
    PtrTo<TableColumnPropertyExpr> property,
    PtrTo<StringLiteral> comment,
    PtrTo<CodecExpr> codec,
    PtrTo<ColumnExpr> ttl)
{
    return PtrTo<TableElementExpr>(new TableElementExpr(ExprType::COLUMN, {name, type, property, comment, codec, ttl}));
}

// static
PtrTo<TableElementExpr> TableElementExpr::createConstraint(PtrTo<Identifier> identifier, PtrTo<ColumnExpr> expr)
{
    return PtrTo<TableElementExpr>(new TableElementExpr(ExprType::CONSTRAINT, {identifier, expr}));
}

// static
PtrTo<TableElementExpr>
TableElementExpr::createIndex(PtrTo<Identifier> name, PtrTo<ColumnExpr> expr, PtrTo<ColumnTypeExpr> type, PtrTo<NumberLiteral> granularity)
{
    return PtrTo<TableElementExpr>(new TableElementExpr(ExprType::INDEX, {name, expr, type, granularity}));
}

TableElementExpr::TableElementExpr(ExprType type, PtrList exprs) : INode(exprs), expr_type(type)
{
}

ASTPtr TableElementExpr::convertToOld() const
{
    switch(expr_type)
    {
        case ExprType::COLUMN:
        {
            auto expr = std::make_shared<ASTColumnDeclaration>();

            expr->name = get<Identifier>(NAME)->getName(); // FIXME: do we have correct nested identifier here already?
            if (has(TYPE))
            {
                expr->type = get(TYPE)->convertToOld();
                expr->children.push_back(expr->type);
            }
            if (has(PROPERTY))
            {
                switch(get<TableColumnPropertyExpr>(PROPERTY)->getType())
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
                expr->default_expression = get(PROPERTY)->convertToOld();
                expr->children.push_back(expr->default_expression);
            }
            if (has(COMMENT))
            {
                expr->comment = get(COMMENT)->convertToOld();
                expr->children.push_back(expr->comment);
            }
            if (has(CODEC))
            {
                expr->codec = get(CODEC)->convertToOld();
                expr->children.push_back(expr->codec);
            }
            if (has(TTL))
            {
                expr->ttl = get(TTL)->convertToOld();
                expr->children.push_back(expr->ttl);
            }

            return expr;
        }
        case ExprType::CONSTRAINT:
        {
            auto expr = std::make_shared<ASTConstraintDeclaration>();

            expr->name = get<Identifier>(NAME)->getName();
            expr->set(expr->expr, get(EXPR)->convertToOld());

            return expr;
        }
        case ExprType::INDEX:
        {
            auto expr = std::make_shared<ASTIndexDeclaration>();

            expr->name = get<Identifier>(NAME)->getName();
            expr->set(expr->expr, get(EXPR)->convertToOld());
            expr->set(expr->type, get(INDEX_TYPE)->convertToOld());
            expr->granularity = get<NumberLiteral>(GRANULARITY)->as<UInt64>().value_or(0); // FIXME: throw exception instead of default.

            return expr;
        }
    }
    __builtin_unreachable();
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCodecArgExpr(ClickHouseParser::CodecArgExprContext *ctx)
{
    auto list = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    return std::make_shared<CodecArgExpr>(visit(ctx->identifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitCodecExpr(ClickHouseParser::CodecExprContext *ctx)
{
    auto list = std::make_shared<CodecArgList>();
    for (auto * arg : ctx->codecArgExpr()) list->push(visit(arg));
    return std::make_shared<CodecExpr>(list);
}

antlrcpp::Any ParseTreeVisitor::visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *ctx)
{
    PtrTo<TableColumnPropertyExpr> property;
    PtrTo<ColumnTypeExpr> type;
    PtrTo<StringLiteral> comment;
    PtrTo<CodecExpr> codec;
    PtrTo<ColumnExpr> ttl;

    if (ctx->tableColumnPropertyExpr()) property = visit(ctx->tableColumnPropertyExpr());
    if (ctx->columnTypeExpr()) type = visit(ctx->columnTypeExpr());
    if (ctx->STRING_LITERAL()) comment = Literal::createString(ctx->STRING_LITERAL());
    if (ctx->codecExpr()) codec = visit(ctx->codecExpr());
    if (ctx->TTL()) ttl = visit(ctx->columnExpr());

    return TableElementExpr::createColumn(visit(ctx->nestedIdentifier()), type, property, comment, codec, ttl);
}

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
    return visit(ctx->tableColumnDfnt());
}

antlrcpp::Any ParseTreeVisitor::visitTableElementExprConstraint(ClickHouseParser::TableElementExprConstraintContext *ctx)
{
    return TableElementExpr::createConstraint(visit(ctx->identifier()), visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitTableElementExprIndex(ClickHouseParser::TableElementExprIndexContext *ctx)
{
    return visit(ctx->tableIndexDfnt());
}

antlrcpp::Any ParseTreeVisitor::visitTableIndexDfnt(ClickHouseParser::TableIndexDfntContext *ctx)
{
    return TableElementExpr::createIndex(
        visit(ctx->nestedIdentifier()),
        visit(ctx->columnExpr()),
        visit(ctx->columnTypeExpr()),
        Literal::createNumber(ctx->DECIMAL_LITERAL()));
}

}
