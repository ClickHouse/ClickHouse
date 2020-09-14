#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SystemQuery : public Query
{
    public:
        static PtrTo<SystemQuery> createFetches(bool stop, PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createMerges(bool stop, PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createSync(PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum class QueryType
        {
            FETCHES,
            MERGES,
            SYNC,
        };

        QueryType query_type;
        bool stop = false;

        SystemQuery(QueryType type, PtrList exprs);
};

}
