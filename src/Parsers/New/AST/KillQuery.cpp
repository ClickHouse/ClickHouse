#include <Parsers/New/AST/KillQuery.h>

#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<KillQuery> KillQuery::createMutation(PtrTo<ClusterClause> cluster, bool sync, bool test, PtrTo<WhereClause> where)
{
    PtrTo<KillQuery> query(new KillQuery(cluster, QueryType::MUTATION, {where}));
    query->sync = sync;
    query->test = test;
    return query;
}

KillQuery::KillQuery(PtrTo<ClusterClause> cluster, QueryType type, PtrList exprs) : DDLQuery(cluster, exprs), query_type(type)
{
}

ASTPtr KillQuery::convertToOld() const
{
    auto query = std::make_shared<ASTKillQueryQuery>();

    query->cluster = cluster_name;

    switch(query_type)
    {
        case QueryType::MUTATION:
            query->type = ASTKillQueryQuery::Type::Mutation;
            query->sync = sync;
            query->test = test;
            query->where_expression = get(WHERE)->convertToOld();
            query->children.push_back(query->where_expression);
            break;
    }

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitKillMutationStmt(ClickHouseParser::KillMutationStmtContext * ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    return KillQuery::createMutation(cluster, !!ctx->SYNC(), !!ctx->TEST(), visit(ctx->whereClause()));
}

}
