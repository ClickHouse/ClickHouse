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

TableArgExpr::TableArgExpr(PtrTo<Literal> literal) : INode{literal}
{
}

TableArgExpr::TableArgExpr(PtrTo<TableFunctionExpr> function) : INode{function}
{
}

TableArgExpr::TableArgExpr(PtrTo<TableIdentifier> identifier) : INode{identifier}
{
}

ASTPtr TableArgExpr::convertToOld() const
{
    return get(EXPR)->convertToOld();
}

// static
PtrTo<TableExpr> TableExpr::createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::ALIAS, {expr, alias}));
}

// static
PtrTo<TableExpr> TableExpr::createFunction(PtrTo<TableFunctionExpr> function)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::FUNCTION, {function}));
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

ASTPtr TableExpr::convertToOld() const
{
    // TODO: SAMPLE and RATIO also goes here somehow

    switch (expr_type)
    {
        case ExprType::ALIAS:
        {
            auto expr = get(EXPR)->convertToOld();
            auto * table_expr = expr->as<ASTTableExpression>();

            if (table_expr->database_and_table_name)
                table_expr->database_and_table_name->setAlias(get<Identifier>(ALIAS)->getName());
            else if (table_expr->table_function)
                table_expr->table_function->setAlias(get<Identifier>(ALIAS)->getName());
            else if (table_expr->subquery)
                table_expr->subquery->setAlias(get<Identifier>(ALIAS)->getName());

            return expr;
        }
        case ExprType::FUNCTION:
        {
            auto expr = std::make_shared<ASTTableExpression>();
            auto func = get(FUNCTION)->convertToOld();

            expr->table_function = func;
            expr->children.push_back(func);

            return expr;
        }
        case ExprType::IDENTIFIER:
        {
            auto expr = std::make_shared<ASTTableExpression>();

            expr->database_and_table_name = get(IDENTIFIER)->convertToOld();
            expr->children.emplace_back(expr->database_and_table_name);

            return expr;
        }
        case ExprType::SUBQUERY:
        {
            auto expr = std::make_shared<ASTTableExpression>();

            expr->subquery = std::make_shared<ASTSubquery>();
            expr->subquery->children.push_back(get(SUBQUERY)->convertToOld());
            expr->children.push_back(expr->subquery);

            return expr;
        }
    }
    __builtin_unreachable();
}

TableExpr::TableExpr(TableExpr::ExprType type, PtrList exprs) : INode(exprs), expr_type(type)
{
}

String TableExpr::dumpInfo() const
{
    switch(expr_type)
    {
        case ExprType::ALIAS: return "ALIAS";
        case ExprType::FUNCTION: return "FUNCTION";
        case ExprType::IDENTIFIER: return "IDENTIFIER";
        case ExprType::SUBQUERY: return "SUBQUERY";
    }
    __builtin_unreachable();
}

TableFunctionExpr::TableFunctionExpr(PtrTo<Identifier> name, PtrTo<TableArgList> args) : INode{name, args}
{
}

ASTPtr TableFunctionExpr::convertToOld() const
{
    auto func = std::make_shared<ASTFunction>();

    func->name = get<Identifier>(NAME)->getName();
    func->arguments = has(ARGS) ? get(ARGS)->convertToOld() : std::make_shared<TableArgList>()->convertToOld();
    func->children.push_back(func->arguments);

    return func;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitTableArgExpr(ClickHouseParser::TableArgExprContext *ctx)
{
    if (ctx->literal()) return std::make_shared<TableArgExpr>(visit(ctx->literal()).as<PtrTo<Literal>>());
    if (ctx->tableFunctionExpr()) return std::make_shared<TableArgExpr>(visit(ctx->tableFunctionExpr()).as<PtrTo<TableFunctionExpr>>());
    if (ctx->tableIdentifier()) return std::make_shared<TableArgExpr>(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableArgList(ClickHouseParser::TableArgListContext * ctx)
{
    auto list = std::make_shared<TableArgList>();
    for (auto * arg : ctx->tableArgExpr()) list->push(visit(arg));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitTableExprAlias(ClickHouseParser::TableExprAliasContext *ctx)
{
    if (ctx->AS()) return TableExpr::createAlias(visit(ctx->tableExpr()), visit(ctx->identifier()));
    else return TableExpr::createAlias(visit(ctx->tableExpr()), visit(ctx->alias()));
}

antlrcpp::Any ParseTreeVisitor::visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *ctx)
{
    return TableExpr::createFunction(visit(ctx->tableFunctionExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx)
{
    return TableExpr::createIdentifier(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *ctx)
{
    return TableExpr::createSubquery(visit(ctx->selectUnionStmt()));
}

antlrcpp::Any ParseTreeVisitor::visitTableFunctionExpr(ClickHouseParser::TableFunctionExprContext *ctx)
{
    auto list = ctx->tableArgList() ? visit(ctx->tableArgList()).as<PtrTo<TableArgList>>() : nullptr;
    return std::make_shared<TableFunctionExpr>(visit(ctx->identifier()), list);
}

}
