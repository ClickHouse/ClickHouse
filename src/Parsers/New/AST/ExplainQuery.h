#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ExplainQuery : public Query
{
    public:
        static PtrTo<ExplainQuery> createExplainAST(PtrTo<Query> query);
        static PtrTo<ExplainQuery> createExplainSyntax(PtrTo<Query> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            QUERY = 0,  // Query
        };

        enum class QueryType
        {
            AST,
            SYNTAX,
        };

        const QueryType query_type;

        ExplainQuery(QueryType type, PtrList exprs);
};

}
