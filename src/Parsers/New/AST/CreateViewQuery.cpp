#include <Parsers/New/AST/CreateViewQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>


namespace DB::AST
{

CreateViewQuery::CreateViewQuery(bool if_not_exists_, PtrTo<TableIdentifier> identifier, PtrTo<SelectUnionQuery> query)
    : if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(query);

    (void)if_not_exists; // TODO
}

}
