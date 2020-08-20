#include <Parsers/New/AST/ShowQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

ShowCreateTableQuery::ShowCreateTableQuery(bool temporary_, PtrTo<TableIdentifier> identifier) : temporary(temporary_)
{
    children.push_back(identifier);
    (void)temporary; // TODO
}

}
