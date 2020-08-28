#include <Parsers/New/AST/CreateTableQuery.h>

#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<SchemaClause> SchemaClause::createDescription(PtrTo<TableElementList> list)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::DESCRIPTION, {list}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsTable(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::TABLE, {identifier}));
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

CreateTableQuery::CreateTableQuery(
    bool temporary_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<SchemaClause> schema,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : temporary(temporary_), if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(schema);
    children.push_back(engine);
    children.push_back(query);

    (void)if_not_exists, (void)temporary; // TODO
}

}

namespace DB
{

using namespace AST;

// TODO: assert(!(ctx->parent->TEMPORARY() ^ ctx->engineClause()))

antlrcpp::Any ParseTreeVisitor::visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx)
{
    auto elems = std::make_shared<TableElementList>();
    for (auto * elem : ctx->tableElementExpr()) elems->append(elem->accept(this).as<PtrTo<TableElementExpr>>());
    return SchemaClause::createDescription(elems);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx)
{
    return SchemaClause::createAsTable(visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx)
{
    return SchemaClause::createAsFunction(
        visit(ctx->identifier()), ctx->tableArgList() ? visit(ctx->tableArgList()).as<PtrTo<TableArgList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx)
{
    return visit(ctx->selectUnionStmt());
}

}
