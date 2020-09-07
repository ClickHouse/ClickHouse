#include <Parsers/New/AST/TruncateQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

TruncateQuery::TruncateQuery(bool temporary_, bool if_exists_, PtrTo<TableIdentifier> identifier)
    : temporary(temporary_), if_exists(if_exists_)
{
    children.push_back(identifier);

    (void) temporary, (void) if_exists; // TODO
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
