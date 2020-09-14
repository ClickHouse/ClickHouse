#include <Parsers/New/AST/JsonExpr.h>

#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

JsonValue::JsonValue(PtrTo<StringLiteral> key, PtrTo<JsonExpr> value)
{
    children.push_back(key);
    children.push_back(value);
}

// static
PtrTo<JsonExpr> JsonExpr::createBoolean(bool value)
{
    PtrTo<JsonExpr> expr(new JsonExpr(ExprType::BOOLEAN, {}));
    expr->value = value;
    return expr;
}

// static
PtrTo<JsonExpr> JsonExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<JsonExpr>(new JsonExpr(ExprType::LITERAL, {literal}));
}

// static
PtrTo<JsonExpr> JsonExpr::createObject(PtrTo<JsonValueList> list)
{
    return PtrTo<JsonExpr>(new JsonExpr(ExprType::OBJECT, {list}));
}

JsonExpr::JsonExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;

    (void) expr_type, (void) value; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitJsonExprBoolean(ClickHouseParser::JsonExprBooleanContext *ctx)
{
    return JsonExpr::createBoolean(!!ctx->JSON_TRUE());
}

antlrcpp::Any ParseTreeVisitor::visitJsonExprLiteral(ClickHouseParser::JsonExprLiteralContext *ctx)
{
    return JsonExpr::createLiteral(visit(ctx->dataLiteral()));
}

antlrcpp::Any ParseTreeVisitor::visitJsonExprObject(ClickHouseParser::JsonExprObjectContext *ctx)
{
    auto list = std::make_shared<JsonValueList>();
    for (auto * value : ctx->jsonValueExpr()) list->append(visit(value));
    return JsonExpr::createObject(list);
}

antlrcpp::Any ParseTreeVisitor::visitJsonValueExpr(ClickHouseParser::JsonValueExprContext * ctx)
{
    return std::make_shared<JsonValue>(Literal::createString(ctx->DATA_STRING_LITERAL()), visit(ctx->jsonExpr()));
}

}
