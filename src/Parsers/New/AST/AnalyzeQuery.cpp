#include <Parsers/New/AST/AnalyzeQuery.h>


namespace DB::AST
{

AnalyzeQuery::AnalyzeQuery(PtrTo<Query> query)
{
    children.push_back(query);
}

}
