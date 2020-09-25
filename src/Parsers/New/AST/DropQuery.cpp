#include <Parsers/New/AST/DropQuery.h>

#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTDropQuery.h>


namespace DB::AST
{

// static
PtrTo<DropQuery> DropQuery::createDropDatabase(bool if_exists, PtrTo<DatabaseIdentifier> identifier)
{
    auto query = PtrTo<DropQuery>(new DropQuery(QueryType::DATABASE, {identifier}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<DropQuery> DropQuery::createDropTable(bool if_exists, bool temporary, PtrTo<TableIdentifier> identifier)
{
    auto query = PtrTo<DropQuery>(new DropQuery(QueryType::TABLE, {identifier}));
    query->if_exists = if_exists;
    query->temporary = temporary;
    return query;
}

DropQuery::DropQuery(QueryType type, PtrList exprs) : DDLQuery(exprs), query_type(type)
{
}

ASTPtr DropQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDropQuery>();

    query->kind = ASTDropQuery::Drop;
    query->if_exists = if_exists;
    query->temporary = temporary;

    // TODO: refactor |ASTQueryWithTableAndOutput| to accept |ASTIdentifier|
    switch(query_type)
    {
        case QueryType::DATABASE:
            query->database = get<DatabaseIdentifier>(NAME)->getName();
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
    return DropQuery::createDropDatabase(!!ctx->EXISTS(), visit(ctx->databaseIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx)
{
    return DropQuery::createDropTable(!!ctx->EXISTS(), !!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

}
