#include <Parsers/New/AST/CreateMaterializedViewQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/SelectUnionQuery.h>


namespace DB::AST
{

CreateMaterializedViewQuery::CreateMaterializedViewQuery(
    bool if_not_exists_, PtrTo<TableIdentifier> identifier, PtrTo<EngineExpr> engine, PtrTo<SelectUnionQuery> query)
    : if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(engine);
    children.push_back(query);

    (void)if_not_exists; // TODO
}

}
