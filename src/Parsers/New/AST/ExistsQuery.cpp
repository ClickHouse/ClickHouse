#include <Parsers/New/AST/ExistsQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

ExistsQuery::ExistsQuery(bool temporary_, PtrTo<TableIdentifier> identifier) : temporary(temporary_)
{
    children.push_back(identifier);

    (void)temporary; // TODO
}

}
