#include <Parsers/New/AST/WatchQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

WatchQuery::WatchQuery(bool events_, PtrTo<TableIdentifier> identifier, PtrTo<NumberLiteral> literal)
    : Query{identifier, literal}, events(events_)
{
}

ASTPtr WatchQuery::convertToOld() const
{
    auto query = std::make_shared<ASTWatchQuery>();

    auto table = std::static_pointer_cast<ASTTableIdentifier>(get(TABLE)->convertToOld());
    query->database = table->getDatabaseName();
    query->table = table->shortName();
    query->uuid = table->uuid;

    query->is_watch_events = events;

    if (has(LIMIT))
        query->limit_length = get(LIMIT)->convertToOld();

    convertToOldPartially(query);

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitWatchStmt(ClickHouseParser::WatchStmtContext *ctx)
{
    auto limit = ctx->DECIMAL_LITERAL() ? Literal::createNumber(ctx->DECIMAL_LITERAL()) : nullptr;
    return std::make_shared<WatchQuery>(!!ctx->EVENTS(), visit(ctx->tableIdentifier()), limit);
}

}
