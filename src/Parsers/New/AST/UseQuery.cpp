#include <Parsers/New/AST/UseQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

UseQuery::UseQuery(PtrTo<DatabaseIdentifier> identifier)
{
    children.push_back(identifier);
}

}
