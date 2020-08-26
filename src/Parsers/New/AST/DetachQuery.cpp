#include <Parsers/New/AST/DetachQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

DetachQuery::DetachQuery(bool if_exists_, PtrTo<TableIdentifier> identifier) : if_exists(if_exists_)
{
    children.push_back(identifier);

    (void)if_exists; // TODO
}

}
