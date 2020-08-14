#include <Parsers/New/AST/ColumnExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>

#include <support/Any.h>



namespace DB::AST
{

// static
PtrTo<ColumnExpr> ColumnExpr::createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ALIAS, {expr, alias}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createAsterisk()
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ASTERISK, {}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnExprList> args)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::FUNCTION, {name, params, args}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createIdentifier(PtrTo<ColumnIdentifier> identifier)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::IDENTIFIER, {identifier}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createLambda(PtrTo<List<Identifier, ','> > params, PtrTo<ColumnExpr> expr)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LAMBDA, {params, expr}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LITERAL, {literal}));
}

ColumnExpr::ColumnExpr(ColumnExpr::ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr ColumnExpr::convertToOld() const
{
    switch (expr_type)
    {
        case ExprType::ALIAS:
        {
            ASTPtr expr = children[EXPR]->convertToOld();

            if (auto * expr_with_alias = dynamic_cast<ASTWithAlias*>(expr.get()))
                expr_with_alias->setAlias(children[ALIAS]->as<Identifier>()->getName());
            else
                throw std::runtime_error("Trying to convert new expression with alias to old one without alias support: " + expr->getID());

            return expr;
        }
        case ExprType::ASTERISK:
            return std::make_shared<ASTAsterisk>();
        case ExprType::FUNCTION:
        {
            auto func = std::make_shared<ASTFunction>();

            func->name = children[NAME]->as<Identifier>()->getName();
            if (children[ARGS])
            {
                func->arguments = children[ARGS]->convertToOld();
                func->children.push_back(func->arguments);
            }
            if (children[PARAMS])
            {
                func->parameters = children[PARAMS]->convertToOld();
                func->children.push_back(func->parameters);
            }

            return func;
        }
        case ExprType::IDENTIFIER:
            return children[IDENTIFIER]->convertToOld();
        case ExprType::LAMBDA:
        {
            auto func = std::make_shared<ASTFunction>();
            auto tuple = std::make_shared<ASTFunction>();

            func->name = "lambda";
            func->arguments = std::make_shared<ASTExpressionList>();
            func->arguments->children.push_back(tuple);
            func->arguments->children.push_back(children[LAMBDA_EXPR]->convertToOld());
            func->children.push_back(func->arguments);

            tuple->name = "tuple";
            tuple->arguments = children[LAMBDA_ARGS]->convertToOld();
            tuple->children.push_back(tuple->arguments);

            return func;
        }
        case ExprType::LITERAL:
            return children[LITERAL]->convertToOld();
    }
}

String ColumnExpr::dumpInfo() const
{
    switch(expr_type)
    {
        case ExprType::ALIAS: return "ALIAS";
        case ExprType::ASTERISK: return "ASTERISK";
        case ExprType::FUNCTION: return "FUNCTION";
        case ExprType::IDENTIFIER: return "IDENTIFIER";
        case ExprType::LAMBDA: return "LAMBDA";
        case ExprType::LITERAL: return "LITERAL";
    }
    __builtin_unreachable();
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitBinaryOp(ClickHouseParser::BinaryOpContext *ctx)
{
    if (ctx->CONCAT()) return String("concat");
    if (ctx->ASTERISK()) return String("multiply");
    if (ctx->SLASH()) return String("divide");
    if (ctx->PERCENT()) return String("modulo");
    if (ctx->PLUS()) return String("plus");
    if (ctx->DASH()) return String("minus");
    if (ctx->EQ_DOUBLE() || ctx->EQ_SINGLE()) return String("equals");
    if (ctx->NOT_EQ()) return String("notEquals");
    if (ctx->LE()) return String("lessOrEquals");
    if (ctx->GE()) return String("greaterOrEquals");
    if (ctx->LT()) return String("less");
    if (ctx->GT()) return String("greater");
    if (ctx->AND()) return String("and");
    if (ctx->OR()) return String("or");
    if (ctx->LIKE())
    {
        if (ctx->NOT()) return String("notLike");
        else return String("like");
    }
    if (ctx->IN())
    {
        if (ctx->GLOBAL())
        {
            if (ctx->NOT()) return String("globalNotIn");
            else return String("globalIn");
        }
        else {
            if (ctx->NOT()) return String("notIn");
            else return String("in");
        }
    }
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *ctx)
{
    if (ctx->columnExpr()) return ctx->columnExpr()->accept(this);
    if (ctx->columnLambdaExpr()) return ctx->columnLambdaExpr()->accept(this);
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnArgList(ClickHouseParser::ColumnArgListContext *ctx)
{
    auto list = std::make_shared<AST::ColumnExprList>();
    for (auto * arg : ctx->columnArgExpr()) list->append(arg->accept(this));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *ctx)
{
    return AST::ColumnExpr::createAlias(ctx->columnExpr()->accept(this), ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("array");
    auto args = ctx->columnExprList() ? ctx->columnExprList()->accept(this).as<AST::PtrTo<AST::ColumnExprList>>() : nullptr;
    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("arrayElement");
    auto args = std::make_shared<AST::ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->append(expr->accept(this));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *)
{
    return AST::ColumnExpr::createAsterisk();
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *ctx)
{
    AST::PtrTo<AST::ColumnExpr> expr1, expr2;

    {
        auto name = std::make_shared<AST::Identifier>(ctx->NOT() ? "lessOrEquals" : "greaterOrEquals");
        auto args = std::make_shared<AST::ColumnExprList>();
        args->append(ctx->columnExpr(0)->accept(this));
        args->append(ctx->columnExpr(1)->accept(this));
        expr1 = AST::ColumnExpr::createFunction(name, nullptr, args);
    }

    {
        auto name = std::make_shared<AST::Identifier>(ctx->NOT() ? "greaterOrEquals" : "lessOrEquals");
        auto args = std::make_shared<AST::ColumnExprList>();
        args->append(ctx->columnExpr(0)->accept(this));
        args->append(ctx->columnExpr(2)->accept(this));
        expr2 = AST::ColumnExpr::createFunction(name, nullptr, args);
    }

    auto name = std::make_shared<AST::Identifier>("and");
    auto args = std::make_shared<AST::ColumnExprList>();

    args->append(expr1);
    args->append(expr2);

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>(ctx->binaryOp()->accept(this).as<String>());
    auto args = std::make_shared<AST::ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->append(expr->accept(this));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *ctx)
{
    auto has_case_expr = (ctx->ELSE() && ctx->columnExpr().size() % 2 == 0) || (!ctx->ELSE() && ctx->columnExpr().size() % 2 == 1);
    auto name = std::make_shared<AST::Identifier>(has_case_expr ? "caseWithExpression" : "multiIf");
    auto args = std::make_shared<AST::ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->append(expr->accept(this));
    // TODO: if (!ctx->ELSE()) args->append(AST::Literal::createNull(???));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
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
    auto name = std::make_shared<AST::Identifier>("extract");
    auto args = std::make_shared<AST::ColumnExprList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));

    return AST::ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx)
{
    return AST::ColumnExpr::createFunction(
        ctx->identifier()->accept(this),
        ctx->columnParamList() ? ctx->columnParamList()->accept(this).as<AST::PtrTo<AST::ColumnParamList>>() : nullptr,
        ctx->columnArgList() ? ctx->columnArgList()->accept(this).as<AST::PtrTo<AST::ColumnExprList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx)
{
    return AST::ColumnExpr::createIdentifier(ctx->columnIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("interval");
    auto args = std::make_shared<AST::ColumnExprList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));

    return AST::ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>(ctx->NOT() ? "isNotNull" : "isNull");
    auto args = std::make_shared<AST::ColumnExprList>();

    args->append(ctx->columnExpr()->accept(this));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx)
{
    auto list = std::make_shared<AST::ColumnExprList>();
    for (auto * expr : ctx->columnExpr()) list->append(expr->accept(this));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx)
{
    return AST::ColumnExpr::createLiteral(ctx->literal()->accept(this).as<AST::PtrTo<AST::Literal>>());
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("if");
    auto args = std::make_shared<AST::ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->append(expr->accept(this));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("trim");
    auto args = std::make_shared<AST::ColumnExprList>();
    auto params = std::make_shared<AST::ColumnParamList>();

    args->append(ctx->columnExpr()->accept(this));
    // TODO: params->append(AST::Literal::createString(???));
    params->append(AST::Literal::createString(ctx->STRING_LITERAL()));

    return AST::ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *ctx)
{
    if (ctx->columnExprList()->columnExpr().size() == 1)
        // Not a tuple - just an expression in parens
        return ctx->columnExprList()->columnExpr(0)->accept(this);

    auto name = std::make_shared<AST::Identifier>("tuple");
    auto args = ctx->columnExprList()->accept(this).as<AST::PtrTo<AST::ColumnExprList>>();

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>("tupleElement");
    auto args = std::make_shared<AST::ColumnExprList>();

    args->append(ctx->columnExpr()->accept(this));
    args->append(AST::ColumnExpr::createLiteral(AST::Literal::createNumber(ctx->INTEGER_LITERAL())));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *ctx)
{
    auto name = std::make_shared<AST::Identifier>(ctx->unaryOp()->accept(this).as<String>());
    auto args = std::make_shared<AST::ColumnExprList>();

    args->append(ctx->columnExpr()->accept(this));

    return AST::ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx)
{
    auto params = std::make_shared<AST::List<AST::Identifier, ','>>();
    for (auto * id : ctx->identifier()) params->append(id->accept(this));
    return AST::ColumnExpr::createLambda(params, ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnParamList(ClickHouseParser::ColumnParamListContext *ctx)
{
    auto param_list = std::make_shared<AST::ColumnParamList>();
    for (auto* param : ctx->literal()) param_list->append(param->accept(this));
    return param_list;
}

antlrcpp::Any ParseTreeVisitor::visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx)
{
    if (ctx->DASH()) return String("negate");
    if (ctx->NOT()) return String("not");
    __builtin_unreachable();
}

}
