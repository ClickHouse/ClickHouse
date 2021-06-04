#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ShowQuery : public Query
{
    public:
        static PtrTo<ShowQuery> createDictionaries(PtrTo<DatabaseIdentifier> from);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            FROM = 0,  // DatabaseIdentifier (optional)
        };

        enum class QueryType
        {
            DICTIONARIES,
        };

        const QueryType query_type;

        ShowQuery(QueryType type, PtrList exprs);
};

}
