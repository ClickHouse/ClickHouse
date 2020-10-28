#include <Parsers/New/AST/ExistsQuery.h>

#include <Parsers/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(bool temporary_, PtrTo<TableIdentifier> identifier) : Query{identifier}, temporary(temporary_)
{
    (void) temporary; // TODO
}

ASTPtr ExistsQuery::convertToOld() const
{
    auto query = std::make_shared<ASTExistsTableQuery>();

    // FIXME: this won't work if table doesn't exist
    auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
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

antlrcpp::Any ParseTreeVisitor::visitExistsStmt(ClickHouseParser::ExistsStmtContext *ctx)
{
    return std::make_shared<ExistsQuery>(!!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

}
