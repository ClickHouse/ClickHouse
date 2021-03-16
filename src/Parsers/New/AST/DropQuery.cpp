#include <Parsers/New/AST/DropQuery.h>

#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTDropQuery.h>


namespace DB::AST
{

// static
PtrTo<DropQuery>
DropQuery::createDropDatabase(bool detach, bool if_exists, PtrTo<DatabaseIdentifier> identifier, PtrTo<ClusterClause> cluster)
{
    auto query = PtrTo<DropQuery>(new DropQuery(cluster, QueryType::DATABASE, {identifier}));
    query->detach = detach;
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<DropQuery>
DropQuery::createDropDictionary(bool detach, bool if_exists, PtrTo<TableIdentifier> identifier, PtrTo<ClusterClause> cluster)
{
    auto query = PtrTo<DropQuery>(new DropQuery(cluster, QueryType::DICTIONARY, {identifier}));
    query->detach = detach;
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<DropQuery>
DropQuery::createDropTable(bool detach, bool if_exists, bool temporary, PtrTo<TableIdentifier> identifier, PtrTo<ClusterClause> cluster)
{
    auto query = PtrTo<DropQuery>(new DropQuery(cluster, QueryType::TABLE, {identifier}));
    query->detach = detach;
    query->if_exists = if_exists;
    query->temporary = temporary;
    return query;
}

DropQuery::DropQuery(PtrTo<ClusterClause> cluster, QueryType type, PtrList exprs) : DDLQuery(cluster, exprs), query_type(type)
{
}

ASTPtr DropQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDropQuery>();

    query->kind = detach ? ASTDropQuery::Detach : ASTDropQuery::Drop;
    query->if_exists = if_exists;
    query->temporary = temporary;
    query->cluster = cluster_name;

    // TODO: refactor |ASTQueryWithTableAndOutput| to accept |ASTIdentifier|
    switch(query_type)
    {
        case QueryType::DATABASE:
            query->database = get<DatabaseIdentifier>(NAME)->getName();
            break;
        case QueryType::DICTIONARY:
            query->is_dictionary = true;
            query->table = get<TableIdentifier>(NAME)->getName();
            if (auto database = get<TableIdentifier>(NAME)->getDatabase())
                query->database = database->getName();
            break;
        case QueryType::TABLE:
        {
            query->table = get<TableIdentifier>(NAME)->getName();
            if (auto database = get<TableIdentifier>(NAME)->getDatabase())
                query->database = database->getName();
            break;
        }
    }

    convertToOldPartially(query);

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    return DropQuery::createDropDatabase(!!ctx->DETACH(), !!ctx->EXISTS(), visit(ctx->databaseIdentifier()), cluster);
}

antlrcpp::Any ParseTreeVisitor::visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    if (ctx->TABLE())
        return DropQuery::createDropTable(!!ctx->DETACH(), !!ctx->EXISTS(), !!ctx->TEMPORARY(), visit(ctx->tableIdentifier()), cluster);
    if (ctx->DICTIONARY())
        return DropQuery::createDropDictionary(!!ctx->DETACH(), !!ctx->EXISTS(), visit(ctx->tableIdentifier()), cluster);
    __builtin_unreachable();
}

}
