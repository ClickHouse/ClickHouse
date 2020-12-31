#include <Parsers/New/AST/AttachQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<AttachQuery> AttachQuery::createDictionary(PtrTo<ClusterClause> clause, PtrTo<TableIdentifier> identifier)
{
    return PtrTo<AttachQuery>(new AttachQuery(clause, QueryType::DICTIONARY, {identifier}));
}

AttachQuery::AttachQuery(PtrTo<ClusterClause> clause, QueryType type, PtrList exprs) : DDLQuery(clause, exprs), query_type(type)
{
}

ASTPtr AttachQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    query->attach = true;

    switch(query_type)
    {
        case QueryType::DICTIONARY:
            query->is_dictionary = true;
            {
                auto table_id = getTableIdentifier(get(NAME)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
    }

    query->cluster = cluster_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAttachDictionaryStmt(ClickHouseParser::AttachDictionaryStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    return AttachQuery::createDictionary(cluster, visit(ctx->tableIdentifier()));
}

}
