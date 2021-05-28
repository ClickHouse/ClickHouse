#include <Parsers/New/AST/CreateDatabaseQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CreateDatabaseQuery::CreateDatabaseQuery(
    PtrTo<ClusterClause> cluster, bool if_not_exists_, PtrTo<DatabaseIdentifier> identifier, PtrTo<EngineExpr> expr)
    : DDLQuery(cluster, {identifier, expr}), if_not_exists(if_not_exists_)
{
}

ASTPtr CreateDatabaseQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    query->if_not_exists = if_not_exists;
    query->database = get<DatabaseIdentifier>(NAME)->getName();
    query->cluster = cluster_name;
    if (has(ENGINE))
    {
        auto engine = std::make_shared<ASTStorage>();
        engine->set(engine->engine, get(ENGINE)->convertToOld());
        query->set(query->storage, engine);
    }
    // TODO: query->uuid

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *ctx)
{
    auto engine = ctx->engineExpr() ? visit(ctx->engineExpr()).as<PtrTo<EngineExpr>>() : nullptr;
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    return std::make_shared<CreateDatabaseQuery>(cluster, !!ctx->IF(), visit(ctx->databaseIdentifier()), engine);
}

}
