#include <Parsers/New/AST/DescribeQuery.h>

#include <Parsers/New/AST/TableExpr.h>


namespace DB::AST
{

DescribeQuery::DescribeQuery(PtrTo<TableExpr> expr)
{
    children.push_back(expr);
}

}
