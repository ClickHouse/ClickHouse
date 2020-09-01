#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SystemQuery : public Query
{
    public:
        static PtrTo<SystemQuery> createSync(PtrTo<TableIdentifier> identifier);

    private:
        enum class QueryType
        {
            SYNC,
        };

        QueryType query_type;

        SystemQuery(QueryType type, PtrList exprs);
};

}
