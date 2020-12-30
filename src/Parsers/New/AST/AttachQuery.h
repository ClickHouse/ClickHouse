#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AttachQuery : public DDLQuery
{
    public:
        static PtrTo<AttachQuery> createDictionary(PtrTo<ClusterClause> clause, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // TableIdentifier
        };

        enum class QueryType
        {
            DICTIONARY,
        };

        const QueryType query_type;

        AttachQuery(PtrTo<ClusterClause> clause, QueryType type, PtrList exprs);
};

}
