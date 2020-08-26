#include <Parsers/New/AST/RenameQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

RenameQuery::RenameQuery(PtrTo<List<TableIdentifier>> list)
{
    children.insert(children.end(), list->begin(), list->end());
}

}
