#include <Parsers/New/AST/CheckQuery.h>

#include <Parsers/ASTCheckQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CheckQuery::CheckQuery(PtrTo<TableIdentifier> identifier)
{
    children.push_back(identifier);
}

ASTPtr CheckQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCheckQuery>();

    // TODO

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
