#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class OptimizeQuery : public DDLQuery
{
    public:
        OptimizeQuery(
            PtrTo<ClusterClause> cluster, PtrTo<TableIdentifier> identifier, PtrTo<PartitionClause> clause, bool final, bool deduplicate);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,  // TableIdentifier
            PARTITION,  // PartitionClause
        };

        const bool final, deduplicate;
};

}
