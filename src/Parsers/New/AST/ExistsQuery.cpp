#include <Parsers/New/AST/ExistsQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(QueryType type, bool temporary_, PtrTo<TableIdentifier> identifier)
    : Query{identifier}, query_type(type), temporary(temporary_)
{
}

ASTPtr ExistsQuery::convertToOld() const
{
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    switch(query_type)
    {
        case QueryType::DICTIONARY:
            query = std::make_shared<ASTExistsDictionaryQuery>();
            break;
        case QueryType::TABLE:
            query = std::make_shared<ASTExistsTableQuery>();
            query->temporary = temporary;
            break;
    }

    // FIXME: this won't work if table doesn't exist
    auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
    query->database = table_id.database_name;
    query->table = table_id.table_name;
    query->uuid = table_id.uuid;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitExistsStmt(ClickHouseParser::ExistsStmtContext *ctx)
{
    auto type = ctx->TABLE() ? ExistsQuery::QueryType::TABLE : ExistsQuery::QueryType::DICTIONARY;
    return std::make_shared<ExistsQuery>(type, !!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

}
