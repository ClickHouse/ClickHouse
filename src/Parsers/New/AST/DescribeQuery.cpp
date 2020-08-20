#include <Parsers/New/AST/DescribeQuery.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

DescribeQuery::DescribeQuery(PtrTo<TableIdentifier> identifier)
{
    children.push_back(identifier);
}

}
