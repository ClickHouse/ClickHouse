#include <Parsers/New/AST/CreateMaterializedViewQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CreateMaterializedViewQuery::CreateMaterializedViewQuery(
    bool attach_,
    bool if_not_exists_,
    bool populate_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<SchemaClause> schema,
    PtrTo<DestinationClause> destination,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : DDLQuery{identifier, schema, destination, engine, query}, attach(attach_), if_not_exists(if_not_exists_), populate(populate_)
{
    assert(!destination != !engine);
}

ASTPtr CreateMaterializedViewQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    {
        auto table_id = getTableIdentifier(get(NAME)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid = table_id.uuid;
    }

    if (has(DESTINATION))
        query->to_table_id = getTableIdentifier(get(DESTINATION)->convertToOld());
    else if (has(ENGINE))
    {
        query->set(query->storage, get(ENGINE)->convertToOld());
        query->is_populate = populate;
    }

    if (has(SCHEMA))
    {
        assert(get<SchemaClause>(SCHEMA)->getType() == SchemaClause::ClauseType::DESCRIPTION);
        query->set(query->columns_list, get(SCHEMA)->convertToOld());
    }

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_materialized_view = true;
    query->set(query->select, get(SUBQUERY)->convertToOld());

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *ctx)
{
    auto schema = ctx->schemaClause() ? visit(ctx->schemaClause()).as<PtrTo<SchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto destination = ctx->destinationClause() ? visit(ctx->destinationClause()).as<PtrTo<DestinationClause>>() : nullptr;
    return std::make_shared<CreateMaterializedViewQuery>(
        !!ctx->ATTACH(),
        !!ctx->IF(),
        !!ctx->POPULATE(),
        visit(ctx->tableIdentifier()),
        schema,
        destination,
        engine,
        visit(ctx->subqueryClause()));
}

antlrcpp::Any ParseTreeVisitor::visitDestinationClause(ClickHouseParser::DestinationClauseContext *ctx)
{
    return std::make_shared<DestinationClause>(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

}
