#include <Parsers/New/AST/TruncateQuery.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TruncateQuery::TruncateQuery(PtrTo<ClusterClause> cluster, bool temporary_, bool if_exists_, PtrTo<TableIdentifier> identifier)
    : DDLQuery(cluster, {identifier}), temporary(temporary_), if_exists(if_exists_)
{
}

ASTPtr TruncateQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDropQuery>();

    query->kind = ASTDropQuery::Truncate;
    query->if_exists = if_exists;
    query->temporary = temporary;
    query->cluster = cluster_name;

    query->table = get<TableIdentifier>(NAME)->getName();
    if (auto database = get<TableIdentifier>(NAME)->getDatabase())
        query->database = database->getName();

    convertToOldPartially(query);

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitTruncateStmt(ClickHouseParser::TruncateStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    return std::make_shared<TruncateQuery>(cluster, !!ctx->TEMPORARY(), !!ctx->IF(), visit(ctx->tableIdentifier()));
}

}
