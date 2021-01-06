#include <Parsers/New/AST/ShowCreateQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
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
PtrTo<ShowCreateQuery> ShowCreateQuery::createDictionary(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<ShowCreateQuery>(new ShowCreateQuery(QueryType::DICTIONARY, {identifier}));
}

// static
PtrTo<ShowCreateQuery> ShowCreateQuery::createTable(bool temporary, PtrTo<TableIdentifier> identifier)
{
    PtrTo<ShowCreateQuery> query(new ShowCreateQuery(QueryType::TABLE, {identifier}));
    query->temporary = temporary;
    return query;
}

ShowCreateQuery::ShowCreateQuery(QueryType type, PtrList exprs) : Query(exprs), query_type(type)
{
}

ASTPtr ShowCreateQuery::convertToOld() const
{
    switch(query_type)
    {
        case QueryType::DATABASE:
        {
            auto query = std::make_shared<ASTShowCreateDatabaseQuery>();
            query->database = get<DatabaseIdentifier>(IDENTIFIER)->getName();
            return query;
        }
        case QueryType::DICTIONARY:
        {
            auto query = std::make_shared<ASTShowCreateDictionaryQuery>();
            auto table_id = getTableIdentifier(get(IDENTIFIER)->convertToOld());

            query->database = table_id.database_name;
            query->table = table_id.table_name;
            query->uuid = table_id.uuid;

            return query;
        }
        case QueryType::TABLE:
        {
            auto query = std::make_shared<ASTShowCreateTableQuery>();
            auto table_id = getTableIdentifier(get(IDENTIFIER)->convertToOld());

            query->database = table_id.database_name;
            query->table = table_id.table_name;
            query->uuid = table_id.uuid;
            query->temporary = temporary;

            return query;
        }
    }
    __builtin_unreachable();
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitShowCreateDatabaseStmt(ClickHouseParser::ShowCreateDatabaseStmtContext *ctx)
{
    return ShowCreateQuery::createDatabase(visit(ctx->databaseIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitShowCreateDictionaryStmt(ClickHouseParser::ShowCreateDictionaryStmtContext * ctx)
{
    return ShowCreateQuery::createDictionary(visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *ctx)
{
    return ShowCreateQuery::createTable(!!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

}
