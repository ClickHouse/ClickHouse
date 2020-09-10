#include <Parsers/New/AST/CreateViewQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

CreateViewQuery::CreateViewQuery(bool if_not_exists_, PtrTo<TableIdentifier> identifier, PtrTo<SelectUnionQuery> query)
    : if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(query);

    (void)if_not_exists; // TODO
}

ASTPtr CreateViewQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    // TODO

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *ctx)
{
    return std::make_shared<CreateViewQuery>(!!ctx->IF(), visit(ctx->tableIdentifier()), visit(ctx->subqueryClause()));
}

}
