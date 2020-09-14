#include <Parsers/New/AST/ExistsQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(bool temporary_, PtrTo<TableIdentifier> identifier) : temporary(temporary_)
{
    children.push_back(identifier);

    (void)temporary; // TODO
}

ASTPtr ExistsQuery::convertToOld() const
{
    auto query = std::make_shared<ASTExistsTableQuery>();

    // TODO

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
