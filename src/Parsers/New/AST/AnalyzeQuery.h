#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class AnalyzeQuery : public Query
{
    public:
        explicit AnalyzeQuery(PtrTo<Query> query);
};

}
