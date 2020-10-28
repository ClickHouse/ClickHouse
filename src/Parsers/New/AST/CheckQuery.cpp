#include <Parsers/New/AST/CheckQuery.h>

#include <Parsers/StorageID.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CheckQuery::CheckQuery(PtrTo<TableIdentifier> identifier) : Query{identifier}
{
}

ASTPtr CheckQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCheckQuery>();

    auto table_id = getTableIdentifier(get(NAME)->convertToOld());
    query->database = table_id.database_name;
    query->table = table_id.table_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCheckStmt(ClickHouseParser::CheckStmtContext *ctx)
{
    return std::make_shared<CheckQuery>(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

}
