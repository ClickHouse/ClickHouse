#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ExplainQuery : public Query
{
    public:
        explicit ExplainQuery(PtrTo<Query> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            QUERY = 0,  // Query
        };
};

}
