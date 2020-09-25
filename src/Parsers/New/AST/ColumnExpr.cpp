#include <Parsers/New/AST/ColumnExpr.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ColumnExpr> ColumnExpr::createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ALIAS, {expr, alias}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createAsterisk(PtrTo<TableIdentifier> identifier, bool single_column)
{
    auto expr = PtrTo<ColumnExpr>(new ColumnExpr(ExprType::ASTERISK, {identifier}));
    expr->expect_single_column = single_column;
    return expr;
}

// static
PtrTo<ColumnExpr> ColumnExpr::createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnExprList> args)
{
    // FIXME: make sure that all function names are camel-case.
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::FUNCTION, {name, params, args}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createIdentifier(PtrTo<ColumnIdentifier> identifier)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::IDENTIFIER, {identifier}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createLambda(PtrTo<List<Identifier>> params, PtrTo<ColumnExpr> expr)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LAMBDA, {params, expr}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LITERAL, {literal}));
}

// static
PtrTo<ColumnExpr> ColumnExpr::createSubquery(PtrTo<SelectUnionQuery> query, bool scalar)
{
    if (scalar) query->shouldBeScalar();
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::SUBQUERY, {query}));
}

ColumnExpr::ColumnExpr(ColumnExpr::ExprType type, PtrList exprs) : INode(exprs), expr_type(type)
{
}

ASTPtr ColumnExpr::convertToOld() const
{
    switch (expr_type)
    {
        case ExprType::ALIAS:
        {
            ASTPtr expr = get(EXPR)->convertToOld();

            if (auto * expr_with_alias = dynamic_cast<ASTWithAlias*>(expr.get()))
                expr_with_alias->setAlias(get<Identifier>(ALIAS)->getName());
            else
                throw std::runtime_error("Trying to convert new expression with alias to old one without alias support: " + expr->getID());

            return expr;
        }
        case ExprType::ASTERISK:
            return std::make_shared<ASTAsterisk>();
        case ExprType::FUNCTION:
        {
            auto func = std::make_shared<ASTFunction>();

            func->name = get<Identifier>(NAME)->getName();
            if (has(ARGS))
            {
                func->arguments = get(ARGS)->convertToOld();
                func->children.push_back(func->arguments);
            }
            if (has(PARAMS))
            {
                func->parameters = get(PARAMS)->convertToOld();
                func->children.push_back(func->parameters);
            }

            return func;
        }
        case ExprType::IDENTIFIER:
            return get(IDENTIFIER)->convertToOld();
        case ExprType::LAMBDA:
        {
            auto func = std::make_shared<ASTFunction>();
            auto tuple = std::make_shared<ASTFunction>();

            func->name = "lambda";
            func->arguments = std::make_shared<ASTExpressionList>();
            func->arguments->children.push_back(tuple);
            func->arguments->children.push_back(get(LAMBDA_EXPR)->convertToOld());
            func->children.push_back(func->arguments);

            tuple->name = "tuple";
            tuple->arguments = get(LAMBDA_ARGS)->convertToOld();
            tuple->children.push_back(tuple->arguments);

            return func;
        }
        case ExprType::LITERAL:
            return get(LITERAL)->convertToOld();
        case ExprType::SUBQUERY:
        {
            auto subquery = std::make_shared<ASTSubquery>();
            subquery->children.push_back(get(SUBQUERY)->convertToOld());
            return subquery;
        }
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
        case ExprType::SUBQUERY: return "SUBQUERY";
    }
    __builtin_unreachable();
}

}

namespace DB
{

using namespace AST;

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
    if (ctx->columnExpr()) return visit(ctx->columnExpr());
    if (ctx->columnLambdaExpr()) return visit(ctx->columnLambdaExpr());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnArgList(ClickHouseParser::ColumnArgListContext *ctx)
{
    auto list = std::make_shared<ColumnExprList>();
    for (auto * arg : ctx->columnArgExpr()) list->push(visit(arg));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *ctx)
{
    return ColumnExpr::createAlias(visit(ctx->columnExpr()), visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *ctx)
{
    auto name = std::make_shared<Identifier>("array");
    auto args = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *ctx)
{
    auto name = std::make_shared<Identifier>("arrayElement");
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *ctx)
{
    return ColumnExpr::createAsterisk(
        ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr, true);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *ctx)
{
    PtrTo<ColumnExpr> expr1, expr2;

    {
        auto name = std::make_shared<Identifier>(ctx->NOT() ? "lessOrEquals" : "greaterOrEquals");
        auto args = std::make_shared<ColumnExprList>();
        args->push(visit(ctx->columnExpr(0)));
        args->push(visit(ctx->columnExpr(1)));
        expr1 = ColumnExpr::createFunction(name, nullptr, args);
    }

    {
        auto name = std::make_shared<Identifier>(ctx->NOT() ? "greaterOrEquals" : "lessOrEquals");
        auto args = std::make_shared<ColumnExprList>();
        args->push(visit(ctx->columnExpr(0)));
        args->push(visit(ctx->columnExpr(2)));
        expr2 = ColumnExpr::createFunction(name, nullptr, args);
    }

    auto name = std::make_shared<Identifier>("and");
    auto args = std::make_shared<ColumnExprList>();

    args->push(expr1);
    args->push(expr2);

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *ctx)
{
    auto name = std::make_shared<Identifier>(visit(ctx->binaryOp()).as<String>());
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *ctx)
{
    auto has_case_expr = (ctx->ELSE() && ctx->columnExpr().size() % 2 == 0) || (!ctx->ELSE() && ctx->columnExpr().size() % 2 == 1);
    auto name = std::make_shared<Identifier>(has_case_expr ? "caseWithExpression" : "multiIf");
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));
    if (!ctx->ELSE()) args->push(ColumnExpr::createLiteral(Literal::createNull()));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *ctx)
{
    auto args = std::make_shared<ColumnExprList>();
    auto params = std::make_shared<ColumnParamList>();

    args->push(visit(ctx->columnExpr()));
    // TODO: params->append(Literal::createString(???));

    return ColumnExpr::createFunction(std::make_shared<Identifier>("cast"), params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *ctx)
{
    auto name = std::make_shared<Identifier>("extract");
    auto args = std::make_shared<ColumnExprList>();
    auto params = std::make_shared<ColumnParamList>();

    args->push(visit(ctx->columnExpr()));
    // TODO: params->append(Literal::createString(???));

    return ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx)
{
    auto params = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    auto args = ctx->columnArgList() ? visit(ctx->columnArgList()).as<PtrTo<ColumnExprList>>() : nullptr;
    return ColumnExpr::createFunction(visit(ctx->identifier()), params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx)
{
    return ColumnExpr::createIdentifier(visit(ctx->columnIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx)
{
    auto name = std::make_shared<Identifier>("interval");
    auto args = std::make_shared<ColumnExprList>();
    auto params = std::make_shared<ColumnParamList>();

    args->push(visit(ctx->columnExpr()));
    // TODO: params->append(Literal::createString(???));

    return ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *ctx)
{
    auto name = std::make_shared<Identifier>(ctx->NOT() ? "isNotNull" : "isNull");
    auto args = std::make_shared<ColumnExprList>();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx)
{
    auto list = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->columnsExpr()) list->push(visit(expr));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx)
{
    return ColumnExpr::createLiteral(visit(ctx->literal()).as<PtrTo<Literal>>());
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *ctx)
{
    return visit(ctx->columnExpr());
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext *ctx)
{
    // IN-operator is special since it accepts non-scalar subqueries on the right side.
    auto * parent = dynamic_cast<ClickHouseParser::ColumnExprBinaryOpContext*>(ctx->parent);
    return ColumnExpr::createSubquery(visit(ctx->selectUnionStmt()), !(parent && parent->binaryOp()->IN()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprSubstring(ClickHouseParser::ColumnExprSubstringContext *ctx)
{
    auto name = std::make_shared<Identifier>("substring");
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *ctx)
{
    auto name = std::make_shared<Identifier>("if");
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *ctx)
{
    auto name = std::make_shared<Identifier>("trim");
    auto args = std::make_shared<ColumnExprList>();
    auto params = std::make_shared<ColumnParamList>();

    args->push(visit(ctx->columnExpr()));
    // TODO: params->append(Literal::createString(???));
    params->push(ColumnExpr::createLiteral(Literal::createString(ctx->STRING_LITERAL())));

    return ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *ctx)
{
    auto name = std::make_shared<Identifier>("tuple");
    auto args = visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>();
    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *ctx)
{
    auto name = std::make_shared<Identifier>("tupleElement");
    auto args = std::make_shared<ColumnExprList>();

    args->push(visit(ctx->columnExpr()));
    args->push(ColumnExpr::createLiteral(Literal::createNumber(ctx->INTEGER_LITERAL())));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *ctx)
{
    auto name = std::make_shared<Identifier>(visit(ctx->unaryOp()).as<String>());
    auto args = std::make_shared<ColumnExprList>();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx)
{
    auto params = std::make_shared<List<Identifier>>();
    for (auto * id : ctx->identifier()) params->push(visit(id));
    return ColumnExpr::createLambda(params, visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnsExprAsterisk(ClickHouseParser::ColumnsExprAsteriskContext *ctx)
{
    return ColumnExpr::createAsterisk(
        ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr, false);
}

antlrcpp::Any ParseTreeVisitor::visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *ctx)
{
    return ColumnExpr::createSubquery(visit(ctx->selectUnionStmt()), false);
}

antlrcpp::Any ParseTreeVisitor::visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *ctx)
{
    return visit(ctx->columnExpr());
}

antlrcpp::Any ParseTreeVisitor::visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx)
{
    if (ctx->DASH()) return String("negate");
    if (ctx->NOT()) return String("not");
    __builtin_unreachable();
}

}
