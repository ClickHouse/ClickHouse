#include <Parsers/New/AST/ShowCreateQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB::AST
{

// static
PtrTo<ShowCreateQuery> ShowCreateQuery::createDatabase(PtrTo<DatabaseIdentifier> identifier)
{
    return PtrTo<ShowCreateQuery>(new ShowCreateQuery(QueryType::DATABASE, {identifier}));
}

// static
PtrTo<ShowCreateQuery> ShowCreateQuery::createTable(bool temporary, PtrTo<TableIdentifier> identifier)
{
    PtrTo<ShowCreateQuery> query(new ShowCreateQuery(QueryType::TABLE, {identifier}));
    query->temporary = temporary;
    return query;
}

ShowCreateQuery::ShowCreateQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;

    (void)query_type, (void)temporary; // TODO
}

ASTPtr ShowCreateQuery::convertToOld() const
{
    auto query = std::make_shared<ASTShowCreateTableQuery>();

    // TODO

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitShowCreateDatabaseStmt(ClickHouseParser::ShowCreateDatabaseStmtContext *ctx)
{
    return ShowCreateQuery::createDatabase(visit(ctx->databaseIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *ctx)
{
    return ShowCreateQuery::createTable(!!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

}
