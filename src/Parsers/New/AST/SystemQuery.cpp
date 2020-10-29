#include <Parsers/New/AST/SystemQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/StorageID.h>


namespace DB::AST
{

// static
PtrTo<SystemQuery> SystemQuery::createDistributedSends(bool stop, PtrTo<TableIdentifier> identifier)
{
    PtrTo<SystemQuery> query(new SystemQuery(QueryType::DISTRIBUTED_SENDS, {identifier}));
    query->stop = stop;
    return query;
}

// static
PtrTo<SystemQuery> SystemQuery::createFetches(bool stop, PtrTo<TableIdentifier> identifier)
{
    PtrTo<SystemQuery> query(new SystemQuery(QueryType::FETCHES, {identifier}));
    query->stop = stop;
    return query;
}

// static
PtrTo<SystemQuery> SystemQuery::createFlushDistributed(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<SystemQuery>(new SystemQuery(QueryType::FLUSH_DISTRIBUTED, {identifier}));
}

// static
PtrTo<SystemQuery> SystemQuery::createFlushLogs()
{
    return PtrTo<SystemQuery>(new SystemQuery(QueryType::FLUSH_LOGS, {}));
}

// static
PtrTo<SystemQuery> SystemQuery::createMerges(bool stop, PtrTo<TableIdentifier> identifier)
{
    PtrTo<SystemQuery> query(new SystemQuery(QueryType::MERGES, {identifier}));
    query->stop = stop;
    return query;
}

// static
PtrTo<SystemQuery> SystemQuery::createSyncReplica(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<SystemQuery>(new SystemQuery(QueryType::SYNC_REPLICA, {identifier}));
}

SystemQuery::SystemQuery(QueryType type, PtrList exprs) : Query(exprs), query_type(type)
{
}

ASTPtr SystemQuery::convertToOld() const
{
    auto query = std::make_shared<ASTSystemQuery>();

    switch(query_type)
    {
        case QueryType::DISTRIBUTED_SENDS:
            query->type = stop ? ASTSystemQuery::Type::STOP_DISTRIBUTED_SENDS : ASTSystemQuery::Type::START_DISTRIBUTED_SENDS;
            {
                auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
        case QueryType::FETCHES:
            query->type = stop ? ASTSystemQuery::Type::STOP_FETCHES : ASTSystemQuery::Type::START_FETCHES;
            {
                auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
        case QueryType::FLUSH_DISTRIBUTED:
            query->type = ASTSystemQuery::Type::FLUSH_DISTRIBUTED;
            {
                auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
        case QueryType::FLUSH_LOGS:
            query->type = ASTSystemQuery::Type::FLUSH_LOGS;
            break;
        case QueryType::MERGES:
            query->type = stop ? ASTSystemQuery::Type::STOP_MERGES : ASTSystemQuery::Type::START_MERGES;
            {
                auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
        case QueryType::SYNC_REPLICA:
            query->type = ASTSystemQuery::Type::SYNC_REPLICA;
            {
                auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
                query->database = table_id.database_name;
                query->table = table_id.table_name;
            }
            break;
    }

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitSystemStmt(ClickHouseParser::SystemStmtContext *ctx)
{
    if (ctx->FLUSH() && ctx->DISTRIBUTED()) return SystemQuery::createFlushDistributed(visit(ctx->tableIdentifier()));
    if (ctx->FLUSH() && ctx->LOGS()) return SystemQuery::createFlushLogs();
    if (ctx->DISTRIBUTED() && ctx->SENDS()) return SystemQuery::createDistributedSends(!!ctx->STOP(), visit(ctx->tableIdentifier()));
    if (ctx->FETCHES()) return SystemQuery::createFetches(!!ctx->STOP(), visit(ctx->tableIdentifier()));
    if (ctx->MERGES()) return SystemQuery::createMerges(!!ctx->STOP(), visit(ctx->tableIdentifier()));
    if (ctx->SYNC() && ctx->REPLICA()) return SystemQuery::createSyncReplica(visit(ctx->tableIdentifier()));
    __builtin_unreachable();
}

}
