#include <Parsers/New/AST/TruncateQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

TruncateQuery::TruncateQuery(bool temporary_, bool if_exists_, PtrTo<TableIdentifier> identifier)
    : temporary(temporary_), if_exists(if_exists_)
{
    children.push_back(identifier);

    (void) temporary, (void) if_exists; // TODO
}

}
