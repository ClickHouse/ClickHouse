#include <Parsers/New/AST/TableExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB::AST
{

TableArgExpr::TableArgExpr(PtrTo<Literal> literal)
{
    children.push_back(literal);
}

TableArgExpr::TableArgExpr(PtrTo<TableExpr> expr)
{
    children.push_back(expr);
}

// static
PtrTo<TableExpr> TableExpr::createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::ALIAS, {expr, alias}));
}

// static
PtrTo<TableExpr> TableExpr::createFunction(PtrTo<Identifier> name, PtrTo<TableArgList> args)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::FUNCTION, {name, args}));
}

// static
PtrTo<TableExpr> TableExpr::createIdentifier(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::IDENTIFIER, {identifier}));
}

// static
PtrTo<TableExpr> TableExpr::createSubquery(PtrTo<SelectUnionQuery> subquery)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::SUBQUERY, {subquery}));
}

TableExpr::TableExpr(TableExpr::ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr TableExpr::convertToOld() const
{
    // TODO: SAMPLE and RATIO also goes here somehow

    switch (expr_type)
    {
        case ExprType::ALIAS:
        {
            auto expr = children[EXPR]->convertToOld();
            auto * table_expr = expr->as<ASTTableExpression>();

            if (table_expr->database_and_table_name)
                table_expr->database_and_table_name->setAlias(children[ALIAS]->as<Identifier>()->getName());
            else if (table_expr->table_function)
                table_expr->table_function->setAlias(children[ALIAS]->as<Identifier>()->getName());
            else if (table_expr->subquery)
                table_expr->subquery->setAlias(children[ALIAS]->as<Identifier>()->getName());

            return expr;
        }
        case ExprType::FUNCTION:
        {
            auto expr = std::make_shared<ASTTableExpression>();
            auto func = std::make_shared<ASTFunction>();

            expr->table_function = func;
            expr->children.push_back(func);

            func->name = children[NAME]->as<Identifier>()->getName();
            func->arguments = children[ARGS] ? children[ARGS]->convertToOld() : nullptr;
            func->children.push_back(func->arguments);

            return expr;
        }
        case ExprType::IDENTIFIER:
        {
            auto expr = std::make_shared<ASTTableExpression>();

            expr->database_and_table_name = children[IDENTIFIER]->convertToOld();
            expr->children.emplace_back(expr->database_and_table_name);

            return expr;
        }
        case ExprType::SUBQUERY:
        {
            auto expr = std::make_shared<ASTTableExpression>();

            expr->subquery = std::make_shared<ASTSubquery>();
            expr->subquery->children.push_back(children[SUBQUERY]->convertToOld());
            expr->children.push_back(expr->subquery);

            return expr;
        }
    }
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitTableArgExpr(ClickHouseParser::TableArgExprContext *ctx)
{
    if (ctx->literal()) return std::make_shared<TableArgExpr>(visit(ctx->literal()).as<PtrTo<Literal>>());
    if (ctx->tableExpr()) return std::make_shared<TableArgExpr>(visit(ctx->tableExpr()).as<PtrTo<TableExpr>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableArgList(ClickHouseParser::TableArgListContext * ctx)
{
    auto list = std::make_shared<TableArgList>();
    for (auto * arg : ctx->tableArgExpr()) list->append(visit(arg));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitTableExprAlias(ClickHouseParser::TableExprAliasContext *ctx)
{
    return TableExpr::createAlias(visit(ctx->tableExpr()), visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *ctx)
{
    return TableExpr::createFunction(
        visit(ctx->identifier()), ctx->tableArgList() ? visit(ctx->tableArgList()).as<PtrTo<TableArgList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx)
{
    return TableExpr::createIdentifier(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *ctx)
{
    return TableExpr::createSubquery(visit(ctx->selectUnionStmt()));
}

}
