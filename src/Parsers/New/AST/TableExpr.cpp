#include <Parsers/New/AST/TableExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB::AST
{

// static
PtrTo<TableExpr> TableExpr::createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::ALIAS, {expr, alias}));
}

// static
PtrTo<TableExpr> TableExpr::createFunction(PtrTo<Identifier> name, PtrList args)
{
    args.insert(args.begin(), name);
    return PtrTo<TableExpr>(new TableExpr(ExprType::FUNCTION, args));
}

// static
PtrTo<TableExpr> TableExpr::createIdentifier(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::IDENTIFIER, {identifier}));
}

// static
PtrTo<TableExpr> TableExpr::createSubquery(PtrTo<SelectStmt> subquery)
{
    return PtrTo<TableExpr>(new TableExpr(ExprType::SUBQUERY, {subquery}));
}

TableExpr::TableExpr(TableExpr::ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr TableExpr::convertToOld() const
{
    auto expr = std::make_shared<ASTTableExpression>();

    // TODO: SAMPLE and RATIO also goes here somehow

    switch (expr_type)
    {
        case ExprType::IDENTIFIER:
            expr->database_and_table_name = children[IDENTIFIER]->convertToOld();
            expr->children.emplace_back(expr->database_and_table_name);
            break;
        default:
            throw std::logic_error("Table expressions other than Identifier are not supported for now");
    }

    return expr;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitTableArgExpr(ClickHouseParser::TableArgExprContext *ctx)
{
    if (ctx->literal()) return ctx->literal()->accept(this);
    if (ctx->tableIdentifier()) return ctx->tableIdentifier()->accept(this);
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableArgList(ClickHouseParser::TableArgListContext *)
{
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitTableExprAlias(ClickHouseParser::TableExprAliasContext *ctx)
{
    return AST::TableExpr::createAlias(ctx->tableExpr()->accept(this), ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *ctx)
{
    AST::PtrList args;

    if (ctx->tableArgList())
        for (auto * arg : ctx->tableArgList()->tableArgExpr()) args.emplace_back(arg->accept(this));

    return AST::TableExpr::createFunction(ctx->identifier()->accept(this), args);
}

antlrcpp::Any ParseTreeVisitor::visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx)
{
    return AST::TableExpr::createIdentifier(ctx->tableIdentifier()->accept(this).as<AST::PtrTo<AST::TableIdentifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *ctx)
{
    return AST::TableExpr::createSubquery(ctx->selectStmt()->accept(this));
}

}
