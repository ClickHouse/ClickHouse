#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class OptimizeQuery : public DDLQuery
{
    public:
        OptimizeQuery(PtrTo<TableIdentifier> identifier, PtrTo<PartitionExprList> list, bool final, bool deduplicate);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,
        };

        const bool final, deduplicate;
};

}
