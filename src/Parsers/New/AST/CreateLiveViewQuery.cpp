#include <Parsers/New/AST/CreateLiveViewQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CreateLiveViewQuery::CreateLiveViewQuery(
    PtrTo<ClusterClause> cluster,
    bool attach_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<UUIDClause> uuid,
    PtrTo<NumberLiteral> timeout,
    PtrTo<DestinationClause> destination,
    PtrTo<TableSchemaClause> schema,
    PtrTo<SelectUnionQuery> query)
    : DDLQuery(cluster, {identifier, uuid, timeout, destination, schema, query}), attach(attach_), if_not_exists(if_not_exists_)
{
}

ASTPtr CreateLiveViewQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    {
        auto table_id = getTableIdentifier(get(NAME)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid
            = has(UUID) ? parseFromString<DB::UUID>(get(UUID)->convertToOld()->as<ASTLiteral>()->value.get<String>()) : table_id.uuid;
    }

    if (has(TIMEOUT))
        query->live_view_timeout.emplace(get(TIMEOUT)->convertToOld()->as<ASTLiteral>()->value.get<UInt64>());

    if (has(DESTINATION))
        query->to_table_id = getTableIdentifier(get(DESTINATION)->convertToOld());

    if (has(SCHEMA))
    {
        assert(get<TableSchemaClause>(SCHEMA)->getType() == TableSchemaClause::ClauseType::DESCRIPTION);
        query->set(query->columns_list, get(SCHEMA)->convertToOld());
    }

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_live_view = true;
    query->set(query->select, get(SUBQUERY)->convertToOld());
    query->cluster = cluster_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateLiveViewStmt(ClickHouseParser::CreateLiveViewStmtContext *ctx)
{
    auto uuid = ctx->uuidClause() ? visit(ctx->uuidClause()).as<PtrTo<UUIDClause>>() : nullptr;
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto timeout = ctx->DECIMAL_LITERAL() ? Literal::createNumber(ctx->DECIMAL_LITERAL()) : nullptr;
    auto destination = ctx->destinationClause() ? visit(ctx->destinationClause()).as<PtrTo<DestinationClause>>() : nullptr;
    auto schema = ctx->tableSchemaClause() ? visit(ctx->tableSchemaClause()).as<PtrTo<TableSchemaClause>>() : nullptr;
    if (ctx->TIMEOUT() && !timeout) timeout = Literal::createNumber(std::to_string(DEFAULT_TEMPORARY_LIVE_VIEW_TIMEOUT_SEC));
    return std::make_shared<CreateLiveViewQuery>(
        cluster,
        !!ctx->ATTACH(),
        !!ctx->IF(),
        visit(ctx->tableIdentifier()),
        uuid,
        timeout,
        destination,
        schema,
        visit(ctx->subqueryClause()));
}

}
