#include <Parsers/New/AST/CheckQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

CheckQuery::CheckQuery(PtrTo<TableIdentifier> identifier)
{
    children.push_back(identifier);
}

}
