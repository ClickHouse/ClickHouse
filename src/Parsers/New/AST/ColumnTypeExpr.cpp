#include <Parsers/New/AST/ColumnTypeExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

EnumValue::EnumValue(PtrTo<StringLiteral> name, PtrTo<NumberLiteral> value)
{
    children.push_back(name);
    children.push_back(value);
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createSimple(PtrTo<Identifier> identifier)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::SIMPLE, {identifier}));
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createComplex(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExprList> list)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::COMPLEX, {identifier, list}));
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createEnum(PtrTo<Identifier> identifier, PtrTo<EnumValueList> list)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::ENUM, {identifier, list}));
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createParam(PtrTo<Identifier> identifier, PtrTo<ColumnParamList> list)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::PARAM, {identifier, list}));
}

ColumnTypeExpr::ColumnTypeExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
    (void)expr_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext *ctx)
{
    return ColumnTypeExpr::createSimple(ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext *ctx)
{
    return ColumnTypeExpr::createParam(ctx->identifier()->accept(this), ctx->columnParamList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext *ctx)
{
    auto list = std::make_shared<EnumValueList>();
    for (auto * value : ctx->enumValue()) list->append(value->accept(this));
    return ColumnTypeExpr::createEnum(ctx->identifier()->accept(this), list);
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext *ctx)
{
    auto list = std::make_shared<ColumnTypeExprList>();
    for (auto * expr : ctx->columnTypeExpr()) list->append(expr->accept(this));
    return ColumnTypeExpr::createComplex(ctx->identifier()->accept(this), list);
}

antlrcpp::Any ParseTreeVisitor::visitEnumValue(ClickHouseParser::EnumValueContext *ctx)
{
    return std::make_shared<EnumValue>(Literal::createString(ctx->STRING_LITERAL()), Literal::createNumber(ctx->NUMBER_LITERAL()));
}

}
