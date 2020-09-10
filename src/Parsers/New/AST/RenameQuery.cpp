#include <Parsers/New/AST/RenameQuery.h>

#include <Parsers/ASTRenameQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

RenameQuery::RenameQuery(PtrTo<List<TableIdentifier>> list)
{
    children.insert(children.end(), list->begin(), list->end());
}

ASTPtr RenameQuery::convertToOld() const
{
    auto query = std::make_shared<ASTRenameQuery>();

    // TODO

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitRenameStmt(ClickHouseParser::RenameStmtContext *ctx)
{
    auto list = std::make_shared<List<TableIdentifier>>();
    for (auto * identifier : ctx->tableIdentifier()) list->append(visit(identifier));
    return std::make_shared<RenameQuery>(list);
}

}
