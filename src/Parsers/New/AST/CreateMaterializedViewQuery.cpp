#include <Parsers/New/AST/CreateMaterializedViewQuery.h>

#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

DestinationClause::DestinationClause(PtrTo<TableIdentifier> identifier)
{
    children.push_back(identifier);
}

CreateMaterializedViewQuery::CreateMaterializedViewQuery(
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<SchemaClause> schema,
    PtrTo<DestinationClause> destination,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(schema);
    children.push_back(destination);
    children.push_back(engine);
    children.push_back(query);

    (void)if_not_exists; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDestinationClause(ClickHouseParser::DestinationClauseContext *ctx)
{
    return std::make_shared<DestinationClause>(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

}
