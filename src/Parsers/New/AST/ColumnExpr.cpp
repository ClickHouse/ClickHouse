#include <memory>
#include <stdexcept>
#include <Parsers/New/AST/ColumnExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <support/Any.h>
#include "Parsers/New/AST/fwd_decl.h"


namespace DB::AST
{

ColumnArgExpr::ColumnArgExpr(PtrTo<ColumnExpr> expr) : type(ArgType::EXPR)
{
    children.push_back(expr);
    (void)type; // TODO: remove this.
}

ColumnArgExpr::ColumnArgExpr(PtrTo<ColumnLambdaExpr> expr) : type(ArgType::LAMBDA)
{
    children.push_back(expr);
}

ColumnLambdaExpr::ColumnLambdaExpr(PtrTo<List<Identifier, ','>> params, PtrTo<ColumnExpr> expr)
{
    children.push_back(expr);
    for (const auto & param : *params) children.push_back(param);
}

// static
PtrTo<ColumnExpr> ColumnExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LITERAL, {literal}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createAsterisk()
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ASTERISK, {}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createIdentifier(PtrTo<ColumnIdentifier> identifier)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::IDENTIFIER, {identifier}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createTuple(PtrTo<ColumnExprList> list)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::TUPLE, PtrList(list->begin(), list->end())));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createArray(PtrTo<ColumnExprList> list)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ARRAY, PtrList(list->begin(), list->end())));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createArrayAccess(PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ARRAY_ACCESS, {expr1, expr2}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createTupleAccess(PtrTo<ColumnExpr> expr, PtrTo<NumberLiteral> literal)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::TUPLE_ACCESS, {expr, literal}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createUnaryOp(UnaryOpType op, PtrTo<ColumnExpr> expr)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(op, expr));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createBinaryOp(BinaryOpType op, PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(op, expr1, expr2));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createTernaryOp(PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2, PtrTo<ColumnExpr> expr3)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(TernaryOpType::IF, expr1, expr2, expr3));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createBetween(bool not_op, PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2, PtrTo<ColumnExpr> expr3)
{
    if (not_op) return PtrTo<ColumnExpr>(new ColumnExpr(TernaryOpType::NOT_BETWEEN, expr1, expr2, expr3));
    else return PtrTo<ColumnExpr>(new ColumnExpr(TernaryOpType::BETWEEN, expr1, expr2, expr3));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createCase(
    PtrTo<ColumnExpr> expr, std::list<std::pair<PtrTo<ColumnExpr>, PtrTo<ColumnExpr>>> cases, PtrTo<ColumnExpr> else_expr)
{
    PtrList list = {expr, else_expr};
    for (const auto & pair : cases)
    {
        list.emplace_back(pair.first);
        list.emplace_back(pair.second);
    }
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::CASE, list));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnArgList> args)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::FUNCTION, {name, params, args}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ALIAS, {expr, alias}));
}

ColumnExpr::ColumnExpr(ColumnExpr::ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

ColumnExpr::ColumnExpr(ColumnExpr::UnaryOpType type, Ptr expr) : expr_type(ExprType::UNARY_OP), unary_op_type(type)
{
    children.push_back(expr);
}

ColumnExpr::ColumnExpr(ColumnExpr::BinaryOpType type, Ptr expr1, Ptr expr2) : expr_type(ExprType::BINARY_OP), binary_op_type(type)
{
    children = {expr1, expr2};
}

ColumnExpr::ColumnExpr(ColumnExpr::TernaryOpType type, Ptr expr1, Ptr expr2, Ptr expr3) : expr_type(ExprType::TERNARY_OP), ternary_op_type(type)
{
    children = {expr1, expr2, expr3};
}

ASTPtr ColumnExpr::convertToOld() const
{
    switch (expr_type)
    {
        case ExprType::LITERAL:
            return children[LITERAL]->convertToOld();
        case ExprType::IDENTIFIER:
            return children[IDENTIFIER]->convertToOld();
        default:
            throw std::logic_error("Unsupported type of column expression");
    }
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitBinaryOp(ClickHouseParser::BinaryOpContext *ctx)
{
    if (ctx->CONCAT()) return AST::ColumnExpr::BinaryOpType::CONCAT;
    if (ctx->ASTERISK()) return AST::ColumnExpr::BinaryOpType::MULTIPLY;
    if (ctx->SLASH()) return AST::ColumnExpr::BinaryOpType::DIVIDE;
    if (ctx->PERCENT()) return AST::ColumnExpr::BinaryOpType::MODULO;
    if (ctx->PLUS()) return AST::ColumnExpr::BinaryOpType::PLUS;
    if (ctx->DASH()) return AST::ColumnExpr::BinaryOpType::MINUS;
    if (ctx->EQ_DOUBLE() || ctx->EQ_SINGLE()) return AST::ColumnExpr::BinaryOpType::EQ;
    if (ctx->NOT_EQ()) return AST::ColumnExpr::BinaryOpType::NOT_EQ;
    if (ctx->LE()) return AST::ColumnExpr::BinaryOpType::LE;
    if (ctx->GE()) return AST::ColumnExpr::BinaryOpType::GE;
    if (ctx->LT()) return AST::ColumnExpr::BinaryOpType::LT;
    if (ctx->GT()) return AST::ColumnExpr::BinaryOpType::GT;
    if (ctx->AND()) return AST::ColumnExpr::BinaryOpType::AND;
    if (ctx->OR()) return AST::ColumnExpr::BinaryOpType::OR;
    if (ctx->LIKE())
    {
        if (ctx->NOT()) return AST::ColumnExpr::BinaryOpType::NOT_LIKE;
        else return AST::ColumnExpr::BinaryOpType::LIKE;
    }
    if (ctx->IN())
    {
        if (ctx->GLOBAL())
        {
            if (ctx->NOT()) return AST::ColumnExpr::BinaryOpType::GLOBAL_NOT_IN;
            else return AST::ColumnExpr::BinaryOpType::GLOBAL_IN;
        }
        else {
            if (ctx->NOT()) return AST::ColumnExpr::BinaryOpType::NOT_IN;
            else return AST::ColumnExpr::BinaryOpType::IN;
        }
    }
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *ctx)
{
    if (ctx->columnExpr())
        return std::make_shared<AST::ColumnArgExpr>(ctx->columnExpr()->accept(this).as<AST::PtrTo<AST::ColumnExpr>>());
    if (ctx->columnLambdaExpr())
        return std::make_shared<AST::ColumnArgExpr>(ctx->columnLambdaExpr()->accept(this).as<AST::PtrTo<AST::ColumnLambdaExpr>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnArgList(ClickHouseParser::ColumnArgListContext *ctx)
{
    auto arg_list = std::make_shared<AST::ColumnArgList>();
    for (auto* arg : ctx->columnArgExpr()) arg_list->append(arg->accept(this));
    return arg_list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *ctx)
{
    return AST::ColumnExpr::createAlias(ctx->columnExpr()->accept(this), ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *ctx)
{
    return AST::ColumnExpr::createArray(ctx->columnExprList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *ctx)
{
    return AST::ColumnExpr::createArrayAccess(ctx->columnExpr(0)->accept(this), ctx->columnExpr(1)->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *)
{
    return AST::ColumnExpr::createAsterisk();
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *ctx)
{
    return AST::ColumnExpr::createBetween(
        !!ctx->NOT(), ctx->columnExpr(0)->accept(this), ctx->columnExpr(1)->accept(this), ctx->columnExpr(2)->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *ctx)
{
    return AST::ColumnExpr::createBinaryOp(
        ctx->binaryOp()->accept(this), ctx->columnExpr(0)->accept(this), ctx->columnExpr(1)->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *ctx)
{
    AST::PtrTo<AST::ColumnExpr> first_expr, else_expr;
    std::list<std::pair<AST::PtrTo<AST::ColumnExpr> /* when */, AST::PtrTo<AST::ColumnExpr> /* then */>> cases;

    auto visit_cases = [this, ctx, &cases] (size_t i)
    {
        for (; i + 1 < ctx->columnExpr().size(); i += 2)
            cases.emplace_back(ctx->columnExpr(i)->accept(this), ctx->columnExpr(i + 1)->accept(this));
    };

    if (ctx->ELSE())
    {
        if (ctx->columnExpr().size() % 2 == 0)
        {
            first_expr = ctx->columnExpr(0)->accept(this);
            visit_cases(1);
        }
        else
            visit_cases(0);
        else_expr = ctx->columnExpr().back()->accept(this);
    }
    else
    {
        if (ctx->columnExpr().size() % 2 == 0)
            visit_cases(0);
        else
        {
            first_expr = ctx->columnExpr(0)->accept(this);
            visit_cases(1);
        }
    }

    return AST::ColumnExpr::createCase(first_expr, cases, else_expr);
}

// antlrcpp::Any ParseTreeVisitor::visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *ctx)
// {
//     auto args = std::make_shared<AST::ColumnArgList>();
//     auto params = std::make_shared<AST::ColumnParamList>();

//     args->append(ctx->columnExpr()->accept(this));
//     // TODO: params->append(AST::Literal::createString(???));

//     return AST::ColumnExpr::createFunction(std::make_shared<AST::Identifier>("CAST"), params, args);
// }

antlrcpp::Any ParseTreeVisitor::visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *ctx)
{
    auto args = std::make_shared<AST::ColumnArgList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));

    return AST::ColumnExpr::createFunction(std::make_shared<AST::Identifier>("EXTRACT"), params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx)
{
    return AST::ColumnExpr::createFunction(
        ctx->identifier()->accept(this), ctx->columnParamList()->accept(this), ctx->columnArgList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx)
{
    return AST::ColumnExpr::createIdentifier(ctx->columnIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx)
{
    auto args = std::make_shared<AST::ColumnArgList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));

    return AST::ColumnExpr::createFunction(std::make_shared<AST::Identifier>("INTERVAL"), params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *ctx)
{
    return AST::ColumnExpr::createUnaryOp(
        ctx->NOT() ? AST::ColumnExpr::UnaryOpType::IS_NOT_NULL : AST::ColumnExpr::UnaryOpType::IS_NULL, ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::ColumnExprList>();
    for (auto* expr : ctx->columnExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx)
{
    return AST::ColumnExpr::createLiteral(ctx->literal()->accept(this).as<AST::PtrTo<AST::Literal>>());
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *ctx)
{
    return ctx->columnExpr()->accept(this);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *ctx)
{
    return AST::ColumnExpr::createTernaryOp(
        ctx->columnExpr(0)->accept(this), ctx->columnExpr(1)->accept(this), ctx->columnExpr(2)->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *ctx)
{
    auto args = std::make_shared<AST::ColumnArgList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));
    params->append(AST::Literal::createString(ctx->STRING_LITERAL()));

    return AST::ColumnExpr::createFunction(std::make_shared<AST::Identifier>("TRIM"), params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *ctx)
{
    return AST::ColumnExpr::createTuple(
        ctx->columnExprList() ? ctx->columnExprList()->accept(this).as<AST::PtrTo<AST::ColumnExprList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *ctx)
{
    return AST::ColumnExpr::createTupleAccess(ctx->columnExpr()->accept(this), AST::Literal::createNumber(ctx->NUMBER_LITERAL()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *ctx)
{
    return AST::ColumnExpr::createUnaryOp(ctx->unaryOp()->accept(this), ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx)
{
    auto params = std::make_shared<AST::List<AST::Identifier, ','>>();

    for (auto * id : ctx->identifier()) params->append(id->accept(this));

    return std::make_shared<AST::ColumnLambdaExpr>(params, ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnParamList(ClickHouseParser::ColumnParamListContext *ctx)
{
    auto param_list = std::make_shared<AST::ColumnParamList>();
    for (auto* param : ctx->literal()) param_list->append(param->accept(this));
    return param_list;
}

antlrcpp::Any ParseTreeVisitor::visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx)
{
    if (ctx->DASH()) return AST::ColumnExpr::UnaryOpType::DASH;
    if (ctx->NOT()) return AST::ColumnExpr::UnaryOpType::NOT;
    __builtin_unreachable();
}

}
