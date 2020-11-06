#include <Parsers/New/AST/TruncateQuery.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TruncateQuery::TruncateQuery(bool temporary_, bool if_exists_, PtrTo<TableIdentifier> identifier)
    : DDLQuery{identifier}, temporary(temporary_), if_exists(if_exists_)
{
}

ASTPtr TruncateQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDropQuery>();

    query->kind = ASTDropQuery::Truncate;
    query->if_exists = if_exists;
    query->temporary = temporary;

    query->table = get<TableIdentifier>(NAME)->getName();
    if (auto database = get<TableIdentifier>(NAME)->getDatabase())
        query->database = database->getName();

    convertToOldPartially(query);

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitTruncateStmt(ClickHouseParser::TruncateStmtContext *ctx)
{
    return std::make_shared<TruncateQuery>(!!ctx->TEMPORARY(), !!ctx->IF(), visit(ctx->tableIdentifier()));
}

}
