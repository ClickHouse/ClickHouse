#include <Parsers/New/AST/ColumnTypeExpr.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

EnumValue::EnumValue(PtrTo<StringLiteral> name, PtrTo<NumberLiteral> value) : INode{name, value}
{
}

ASTPtr EnumValue::convertToOld() const
{
    auto func = std::make_shared<ASTFunction>();

    func->name = "equals";
    func->arguments = std::make_shared<ASTExpressionList>();
    func->arguments->children.push_back(get(NAME)->convertToOld());
    func->arguments->children.push_back(get(VALUE)->convertToOld());
    func->children.push_back(func->arguments);

    return func;
}

String EnumValue::toString() const
{
    return fmt::format("{} = {}", get(NAME)->toString(), get(VALUE)->toString());
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createSimple(PtrTo<Identifier> identifier)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::SIMPLE, {identifier}));
}

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createNamed(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExpr> type)
{
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::NAMED, {identifier, type}));
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

// static
PtrTo<ColumnTypeExpr> ColumnTypeExpr::createNested(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExprList> list)
{
    // TODO: assert that |list| must contain only expressions of NAMED type
    return PtrTo<ColumnTypeExpr>(new ColumnTypeExpr(ExprType::NESTED, {identifier, list}));
}

ColumnTypeExpr::ColumnTypeExpr(ExprType type, PtrList exprs) : INode(exprs), expr_type(type)
{
}

ASTPtr ColumnTypeExpr::convertToOld() const
{
    if (expr_type == ExprType::NAMED)
    {
        auto pair = std::make_shared<ASTNameTypePair>();

        pair->name = get<Identifier>(NAME)->getName();
        pair->type = get(TYPE)->convertToOld();
        pair->children.push_back(pair->type);

        return pair;
    }

    auto func = std::make_shared<ASTFunction>();

    func->name = get<Identifier>(NAME)->getName();
    func->no_empty_args = true;
    if (expr_type != ExprType::SIMPLE && has(LIST))
    {
        func->arguments = get(LIST)->convertToOld();
        func->children.push_back(func->arguments);
    }

    return func;
}

String ColumnTypeExpr::toString() const
{
    switch(expr_type)
    {
        case ExprType::SIMPLE:
            return get(NAME)->toString();
        case ExprType::NAMED:
            return get(NAME)->toString() + " " + get(TYPE)->toString();
        case ExprType::COMPLEX:
        case ExprType::ENUM:
        case ExprType::PARAM:
        case ExprType::NESTED:
            return get(NAME)->toString() + "(" + (has(LIST) ? get(LIST)->toString() : "") + ")";
    }
    __builtin_unreachable();
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext *ctx)
{
    return ColumnTypeExpr::createSimple(visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext *ctx)
{
    auto list = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    return ColumnTypeExpr::createParam(visit(ctx->identifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext *ctx)
{
    auto list = std::make_shared<EnumValueList>();
    for (auto * value : ctx->enumValue()) list->push(visit(value));
    return ColumnTypeExpr::createEnum(visit(ctx->identifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext *ctx)
{
    auto list = std::make_shared<ColumnTypeExprList>();
    for (auto * expr : ctx->columnTypeExpr()) list->push(visit(expr));
    return ColumnTypeExpr::createComplex(visit(ctx->identifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitColumnTypeExprNested(ClickHouseParser::ColumnTypeExprNestedContext *ctx)
{
    auto list = std::make_shared<ColumnTypeExprList>();

    for (size_t i = 0; i < ctx->columnTypeExpr().size(); ++i)
        list->push(ColumnTypeExpr::createNamed(visit(ctx->identifier(i + 1)), visit(ctx->columnTypeExpr(i))));

    return ColumnTypeExpr::createNested(visit(ctx->identifier(0)), list);
}

antlrcpp::Any ParseTreeVisitor::visitEnumValue(ClickHouseParser::EnumValueContext *ctx)
{
    return std::make_shared<EnumValue>(Literal::createString(ctx->STRING_LITERAL()), visit(ctx->numberLiteral()));
}

}
