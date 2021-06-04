#include <Parsers/New/AST/OptimizeQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/New/AST/AlterTableQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

OptimizeQuery::OptimizeQuery(PtrTo<ClusterClause> cluster, PtrTo<TableIdentifier> identifier, PtrTo<PartitionClause> clause, bool final_, bool deduplicate_)
    : DDLQuery(cluster, {identifier, clause}), final(final_), deduplicate(deduplicate_)
{
}

ASTPtr OptimizeQuery::convertToOld() const
{
    auto query = std::make_shared<ASTOptimizeQuery>();

    {
        auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid = table_id.uuid;
    }

    if (has(PARTITION))
    {
        query->partition = get(PARTITION)->convertToOld();
        query->children.push_back(query->partition);
    }

    query->final = final;
    query->deduplicate = deduplicate;
    query->cluster = cluster_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto clause = ctx->partitionClause() ? visit(ctx->partitionClause()).as<PtrTo<PartitionClause>>() : nullptr;
    return std::make_shared<OptimizeQuery>(cluster, visit(ctx->tableIdentifier()), clause, !!ctx->FINAL(), !!ctx->DEDUPLICATE());
}

}
