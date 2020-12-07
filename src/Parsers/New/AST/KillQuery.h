#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class KillQuery : public DDLQuery
{
    public:
        static PtrTo<KillQuery> createMutation(PtrTo<ClusterClause> cluster, bool sync, bool test, PtrTo<WhereClause> where);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            WHERE = 0,  // WhereClause
        };

        enum class QueryType
        {
            MUTATION,
        };

        const QueryType query_type;
        bool sync = false, test = false;

        KillQuery(PtrTo<ClusterClause> cluster, QueryType type, PtrList exprs);
};

}
