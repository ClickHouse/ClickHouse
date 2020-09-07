#include <Parsers/New/AST/CreateDatabaseQuery.h>

#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTCreateQuery.h>


namespace DB::AST
{

CreateDatabaseQuery::CreateDatabaseQuery(bool if_not_exists_, PtrTo<DatabaseIdentifier> identifier, PtrTo<EngineExpr> expr) : if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(expr);
}

ASTPtr CreateDatabaseQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    query->if_not_exists = if_not_exists;
    query->database = children[NAME]->as<DatabaseIdentifier>()->getName();
    // TODO: if (cluster) query->cluster = cluster->getName();
    if (children[ENGINE]) query->set(query->storage, children[ENGINE]->convertToOld());
    // TODO: query->uuid

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *ctx)
{
    auto engine = ctx->engineExpr() ? visit(ctx->engineExpr()).as<PtrTo<EngineExpr>>() : nullptr;
    return std::make_shared<CreateDatabaseQuery>(!!ctx->IF(), visit(ctx->databaseIdentifier()), engine);
}

}
