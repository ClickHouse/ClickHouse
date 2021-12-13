#include <Parsers/New/AST/ColumnExpr.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/New/AST/ColumnTypeExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::ErrorCodes
{
    extern int SYNTAX_ERROR;
}

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

    // Flatten some consequent binary operators to a single multi-operator, because they are left-associative.
    if ((name->getName() == "or" || name->getName() == "and") && args && args->size() == 2)
    {
        const auto * left = (*args->begin())->as<ColumnExpr>();
        const auto * right = (*++args->begin())->as<ColumnExpr>();

        if (left && left->getType() == ExprType::FUNCTION && left->getFunctionName() == name->getName())
        {
            auto new_args = std::make_shared<ColumnExprList>();
            for (const auto & arg : left->get(ARGS)->as<ColumnExprList &>())
                new_args->push(std::static_pointer_cast<ColumnExpr>(arg));
            new_args->push(std::static_pointer_cast<ColumnExpr>(*++args->begin()));
            args = new_args;
        }
        else if (right && right->getType() == ExprType::FUNCTION && right->getFunctionName() == name->getName())
        {
            auto new_args = std::make_shared<ColumnExprList>();
            new_args->push(std::static_pointer_cast<ColumnExpr>(*args->begin()));
            for (const auto & arg : right->get(ARGS)->as<ColumnExprList &>())
                new_args->push(std::static_pointer_cast<ColumnExpr>(arg));
            args = new_args;
        }
    }

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
            if (has(TABLE))
            {
                auto expr = std::make_shared<ASTQualifiedAsterisk>();
                expr->children.push_back(get(TABLE)->convertToOld());
                return expr;
            }
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
    __builtin_unreachable();
}

String ColumnExpr::toString() const
{
    switch(expr_type)
    {
        case ExprType::LITERAL: return get(LITERAL)->toString();
        default: return {};
    }
    __builtin_unreachable();
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
    if (ctx->AS()) return ColumnExpr::createAlias(visit(ctx->columnExpr()), visit(ctx->identifier()));
    else return ColumnExpr::createAlias(visit(ctx->columnExpr()), visit(ctx->alias()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprAnd(ClickHouseParser::ColumnExprAndContext *ctx)
{
    auto name = std::make_shared<Identifier>("and");
    auto args = std::make_shared<ColumnExprList>();

    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
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
    auto table = ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr;
    return ColumnExpr::createAsterisk(table, true);
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

    args->push(visit(ctx->columnExpr()));
    args->push(ColumnExpr::createLiteral(Literal::createString(visit(ctx->columnTypeExpr()).as<PtrTo<ColumnTypeExpr>>()->toString())));

    return ColumnExpr::createFunction(std::make_shared<Identifier>("cast"), nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprDate(ClickHouseParser::ColumnExprDateContext *ctx)
{
    auto name = std::make_shared<Identifier>("toDate");
    auto args = std::make_shared<ColumnExprList>();

    args->push(ColumnExpr::createLiteral(Literal::createString(ctx->STRING_LITERAL())));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *ctx)
{
    String name;
    auto args = std::make_shared<ColumnExprList>();

    if (ctx->interval()->SECOND()) name = "toSecond";
    else if (ctx->interval()->MINUTE()) name = "toMinute";
    else if (ctx->interval()->HOUR()) name = "toHour";
    else if (ctx->interval()->DAY()) name = "toDayOfMonth";
    else if (ctx->interval()->WEEK())
        throw Exception(
            "The syntax 'EXTRACT(WEEK FROM date)' is not supported, cannot extract the number of a week", ErrorCodes::SYNTAX_ERROR);
    else if (ctx->interval()->MONTH()) name = "toMonth";
    else if (ctx->interval()->QUARTER()) name = "toQuarter";
    else if (ctx->interval()->YEAR()) name = "toYear";
    else __builtin_unreachable();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(std::make_shared<Identifier>(name), nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx)
{
    auto name = visit(ctx->identifier()).as<PtrTo<Identifier>>();
    auto params = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    auto args = ctx->columnArgList() ? visit(ctx->columnArgList()).as<PtrTo<ColumnExprList>>() : nullptr;

    if (ctx->DISTINCT()) name = std::make_shared<Identifier>(name->getName() + "Distinct");

    return ColumnExpr::createFunction(name, params, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx)
{
    return ColumnExpr::createIdentifier(visit(ctx->columnIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx)
{
    PtrTo<Identifier> name;
    auto args = std::make_shared<ColumnExprList>();

    if (ctx->interval()->SECOND()) name = std::make_shared<Identifier>("toIntervalSecond");
    else if (ctx->interval()->MINUTE()) name = std::make_shared<Identifier>("toIntervalMinute");
    else if (ctx->interval()->HOUR()) name = std::make_shared<Identifier>("toIntervalHour");
    else if (ctx->interval()->DAY()) name = std::make_shared<Identifier>("toIntervalDay");
    else if (ctx->interval()->WEEK()) name = std::make_shared<Identifier>("toIntervalWeek");
    else if (ctx->interval()->MONTH()) name = std::make_shared<Identifier>("toIntervalMonth");
    else if (ctx->interval()->QUARTER()) name = std::make_shared<Identifier>("toIntervalQuarter");
    else if (ctx->interval()->YEAR()) name = std::make_shared<Identifier>("toIntervalYear");
    else __builtin_unreachable();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(name, nullptr, args);
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

antlrcpp::Any ParseTreeVisitor::visitColumnExprNegate(ClickHouseParser::ColumnExprNegateContext *ctx)
{
    auto name = std::make_shared<Identifier>("negate");
    auto args = std::make_shared<ColumnExprList>();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprNot(ClickHouseParser::ColumnExprNotContext *ctx)
{
    auto name = std::make_shared<Identifier>("not");
    auto args = std::make_shared<ColumnExprList>();

    args->push(visit(ctx->columnExpr()));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprOr(ClickHouseParser::ColumnExprOrContext *ctx)
{
    auto name = std::make_shared<Identifier>("or");

    auto args = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *ctx)
{
    return visit(ctx->columnExpr());
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprPrecedence1(ClickHouseParser::ColumnExprPrecedence1Context *ctx)
{
    PtrTo<Identifier> name;
    if (ctx->ASTERISK()) name = std::make_shared<Identifier>("multiply");
    else if (ctx->SLASH()) name = std::make_shared<Identifier>("divide");
    else if (ctx->PERCENT()) name = std::make_shared<Identifier>("modulo");

    auto args = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprPrecedence2(ClickHouseParser::ColumnExprPrecedence2Context *ctx)
{
    PtrTo<Identifier> name;
    if (ctx->PLUS()) name = std::make_shared<Identifier>("plus");
    else if (ctx->DASH()) name = std::make_shared<Identifier>("minus");
    else if (ctx->CONCAT()) name = std::make_shared<Identifier>("concat");

    auto args = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprPrecedence3(ClickHouseParser::ColumnExprPrecedence3Context *ctx)
{
    PtrTo<Identifier> name;
    if (ctx->EQ_DOUBLE() || ctx->EQ_SINGLE()) name = std::make_shared<Identifier>("equals");
    else if (ctx->NOT_EQ()) name = std::make_shared<Identifier>("notEquals");
    else if (ctx->LE()) name = std::make_shared<Identifier>("lessOrEquals");
    else if (ctx->GE()) name = std::make_shared<Identifier>("greaterOrEquals");
    else if (ctx->LT()) name = std::make_shared<Identifier>("less");
    else if (ctx->GT()) name = std::make_shared<Identifier>("greater");
    else if (ctx->LIKE())
    {
        if (ctx->NOT()) name = std::make_shared<Identifier>("notLike");
        else name = std::make_shared<Identifier>("like");
    }
    else if (ctx->ILIKE())
    {
        if (ctx->NOT()) name = std::make_shared<Identifier>("notILike");
        else name = std::make_shared<Identifier>("ilike");
    }
    else if (ctx->IN())
    {
        if (ctx->GLOBAL())
        {
            if (ctx->NOT()) name = std::make_shared<Identifier>("globalNotIn");
            else name = std::make_shared<Identifier>("globalIn");
        }
        else
        {
            if (ctx->NOT()) name = std::make_shared<Identifier>("notIn");
            else name = std::make_shared<Identifier>("in");
        }
    }

    auto args = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->columnExpr()) args->push(visit(expr));

    return ColumnExpr::createFunction(name, nullptr, args);
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext *ctx)
{
    // IN-operator is special since it accepts non-scalar subqueries on the right side.
    auto * parent = dynamic_cast<ClickHouseParser::ColumnExprPrecedence3Context*>(ctx->parent);
    return ColumnExpr::createSubquery(visit(ctx->selectUnionStmt()), !(parent && parent->IN()));
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

antlrcpp::Any ParseTreeVisitor::visitColumnExprTimestamp(ClickHouseParser::ColumnExprTimestampContext *ctx)
{
    auto name = std::make_shared<Identifier>("toDateTime");
    auto args = std::make_shared<ColumnExprList>();

    args->push(ColumnExpr::createLiteral(Literal::createString(ctx->STRING_LITERAL())));

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
    args->push(ColumnExpr::createLiteral(Literal::createNumber(ctx->DECIMAL_LITERAL())));

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
    auto table = ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr;
    return ColumnExpr::createAsterisk(table, false);
}

antlrcpp::Any ParseTreeVisitor::visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *ctx)
{
    return ColumnExpr::createSubquery(visit(ctx->selectUnionStmt()), false);
}

antlrcpp::Any ParseTreeVisitor::visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *ctx)
{
    return visit(ctx->columnExpr());
}

}
