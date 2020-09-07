#include <Parsers/New/AST/ExistsQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(bool temporary_, PtrTo<TableIdentifier> identifier) : temporary(temporary_)
{
    children.push_back(identifier);

    (void)temporary; // TODO
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
