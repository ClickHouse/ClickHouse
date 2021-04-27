#include <Parsers/New/AST/ExistsQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(QueryType type, bool temporary_, PtrList exprs)
    : Query(exprs), query_type(type), temporary(temporary_)
{
}

// static
PtrTo<ExistsQuery> ExistsQuery::createTable(QueryType type, bool temporary, PtrTo<TableIdentifier> identifier)
{
    return PtrTo<ExistsQuery>(new ExistsQuery(type, temporary, {identifier}));
}

// static
PtrTo<ExistsQuery> ExistsQuery::createDatabase(PtrTo<DatabaseIdentifier> identifier)
{
    return PtrTo<ExistsQuery>(new ExistsQuery(QueryType::DATABASE, false, {identifier}));
}

ASTPtr ExistsQuery::convertToOld() const
{
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    switch(query_type)
    {
        case QueryType::DATABASE:
            query = std::make_shared<ASTExistsDatabaseQuery>();
            tryGetIdentifierNameInto(get<DatabaseIdentifier>(IDENTIFIER)->convertToOld(), query->database);
            return query;

        case QueryType::DICTIONARY:
            query = std::make_shared<ASTExistsDictionaryQuery>();
            break;
        case QueryType::TABLE:
            query = std::make_shared<ASTExistsTableQuery>();
            break;
        case QueryType::VIEW:
            query = std::make_shared<ASTExistsViewQuery>();
            break;
    }

    // FIXME: this won't work if table doesn't exist
    auto table_id = getTableIdentifier(get<TableIdentifier>(IDENTIFIER)->convertToOld());
    query->database = table_id.database_name;
    query->table = table_id.table_name;
    query->uuid = table_id.uuid;
    query->temporary = temporary;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitExistsTableStmt(ClickHouseParser::ExistsTableStmtContext *ctx)
{
    ExistsQuery::QueryType type;
    if (ctx->DICTIONARY())
        type = ExistsQuery::QueryType::DICTIONARY;
    else if (ctx->VIEW())
        type = ExistsQuery::QueryType::VIEW;
    else // Query 'EXISTS <table_name>' is interptered as 'EXISTS TABLE <table_name>'
        type = ExistsQuery::QueryType::TABLE;

    return ExistsQuery::createTable(type, !!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitExistsDatabaseStmt(ClickHouseParser::ExistsDatabaseStmtContext *ctx)
{
    return ExistsQuery::createDatabase(visit(ctx->databaseIdentifier()));
}

}
