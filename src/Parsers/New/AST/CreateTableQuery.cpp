#include <Parsers/New/AST/CreateTableQuery.h>

#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<SchemaClause> SchemaClause::createDescription(PtrTo<TableElementList> list, PtrTo<EngineClause> clause)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::DESCRIPTION, {list, clause}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsSubquery(PtrTo<SelectUnionQuery> query, PtrTo<EngineClause> clause)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::SUBQUERY, {query, clause}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsTable(PtrTo<TableIdentifier> identifier, PtrTo<EngineClause> clause)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::TABLE, {identifier, clause}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsFunction(PtrTo<Identifier> identifier, PtrTo<TableArgList> list)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::FUNCTION, {identifier, list}));
}

SchemaClause::SchemaClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;
    (void)clause_type; // TODO
}

CreateTableQuery::CreateTableQuery(bool temporary_, bool if_not_exists_, PtrTo<TableIdentifier> identifier, PtrTo<SchemaClause> clause)
    : temporary(temporary_), if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(clause);
    (void)if_not_exists, (void)temporary; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx)
{
    auto elems = std::make_shared<TableElementList>();
    for (auto * elem : ctx->tableElementExpr()) elems->append(elem->accept(this).as<PtrTo<TableElementExpr>>());

    // TODO: assert(!(ctx->parent->TEMPORARY() ^ ctx->engineClause()))
    return SchemaClause::createDescription(
        elems, ctx->engineClause() ? ctx->engineClause()->accept(this).as<PtrTo<EngineClause>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsSubqueryClause(ClickHouseParser::SchemaAsSubqueryClauseContext *ctx)
{
    return SchemaClause::createAsSubquery(
        ctx->selectUnionStmt()->accept(this), ctx->engineClause() ? ctx->engineClause()->accept(this).as<PtrTo<EngineClause>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx)
{
    return SchemaClause::createAsTable(
        ctx->tableIdentifier()->accept(this), ctx->engineClause() ? ctx->engineClause()->accept(this).as<PtrTo<EngineClause>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx)
{
    return SchemaClause::createAsFunction(
        ctx->identifier()->accept(this), ctx->tableArgList() ? ctx->tableArgList()->accept(this).as<PtrTo<TableArgList>>() : nullptr);
}

}
