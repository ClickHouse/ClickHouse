#include <Parsers/New/AST/CreateViewQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{
CreateViewQuery::CreateViewQuery(
    PtrTo<ClusterClause> cluster,
    bool attach_,
    bool replace_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<TableSchemaClause> clause,
    PtrTo<SelectUnionQuery> query)
    : DDLQuery(cluster, {identifier, clause, query}), attach(attach_), replace(replace_), if_not_exists(if_not_exists_)
{
}

ASTPtr CreateViewQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    {
        auto table_id = getTableIdentifier(get(NAME)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid = table_id.uuid;
    }

    query->attach = attach;
    query->replace_view = replace;
    query->if_not_exists = if_not_exists;
    query->is_view = true;
    query->cluster = cluster_name;

    if (has(SCHEMA)) query->set(query->columns_list, get(SCHEMA)->convertToOld());
    query->set(query->select, get(SUBQUERY)->convertToOld());

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto schema = ctx->tableSchemaClause() ? visit(ctx->tableSchemaClause()).as<PtrTo<TableSchemaClause>>() : nullptr;
    return std::make_shared<CreateViewQuery>(
        cluster, !!ctx->ATTACH(), !!ctx->REPLACE(), !!ctx->IF(), visit(ctx->tableIdentifier()), schema, visit(ctx->subqueryClause()));
}

}
