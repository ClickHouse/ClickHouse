#include <Parsers/New/AST/AlterTableQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<AlterTableClause> AlterTableClause::createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::ADD, {element, after}));
    query->if_not_exists = if_not_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDrop(bool if_exists, PtrTo<Identifier> identifier)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::DROP, {identifier}));
    query->if_exists = if_exists;
    return query;
}

AlterTableClause::AlterTableClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;
    (void)clause_type; // TODO
}

AlterTableQuery::AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<AlterTableClause> clause)
{
    children.push_back(identifier);
    children.push_back(clause);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAlterTableAddClause(ClickHouseParser::AlterTableAddClauseContext *ctx)
{
    auto after = ctx->AFTER() ? ctx->identifier()->accept(this).as<PtrTo<Identifier>>() : nullptr;
    return AlterTableClause::createAdd(!!ctx->IF(), ctx->tableColumnDfnt()->accept(this), after);
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableDropClause(ClickHouseParser::AlterTableDropClauseContext *ctx)
{
    return AlterTableClause::createDrop(!!ctx->IF(), ctx->identifier()->accept(this));
}

}
