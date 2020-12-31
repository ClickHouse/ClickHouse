#include <Parsers/New/AST/CreateMaterializedViewQuery.h>

#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{
CreateMaterializedViewQuery::CreateMaterializedViewQuery(
    PtrTo<ClusterClause> cluster,
    bool attach_,
    bool if_not_exists_,
    bool populate_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<UUIDClause> uuid,
    PtrTo<TableSchemaClause> schema,
    PtrTo<DestinationClause> destination,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : DDLQuery(cluster, {identifier, uuid, schema, destination, engine, query})
    , attach(attach_)
    , if_not_exists(if_not_exists_)
    , populate(populate_)
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
        query->uuid
            = has(UUID) ? parseFromString<DB::UUID>(get(UUID)->convertToOld()->as<ASTLiteral>()->value.get<String>()) : table_id.uuid;
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
        assert(get<TableSchemaClause>(SCHEMA)->getType() == TableSchemaClause::ClauseType::DESCRIPTION);
        query->set(query->columns_list, get(SCHEMA)->convertToOld());
    }

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_materialized_view = true;
    query->set(query->select, get(SUBQUERY)->convertToOld());
    query->cluster = cluster_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *ctx)
{
    auto uuid = ctx->uuidClause() ? visit(ctx->uuidClause()).as<PtrTo<UUIDClause>>() : nullptr;
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto schema = ctx->tableSchemaClause() ? visit(ctx->tableSchemaClause()).as<PtrTo<TableSchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto destination = ctx->destinationClause() ? visit(ctx->destinationClause()).as<PtrTo<DestinationClause>>() : nullptr;
    return std::make_shared<CreateMaterializedViewQuery>(
        cluster,
        !!ctx->ATTACH(),
        !!ctx->IF(),
        !!ctx->POPULATE(),
        visit(ctx->tableIdentifier()),
        uuid,
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
