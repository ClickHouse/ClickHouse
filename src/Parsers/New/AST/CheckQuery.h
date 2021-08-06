#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class CheckQuery : public Query
{
    public:
        CheckQuery(PtrTo<TableIdentifier> identifier, PtrTo<PartitionClause> clause);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,       // TableIdentifier
            PARTITION = 1,  // PartitionClause (optional)
        };
};

}
