#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class TruncateQuery : public DDLQuery
{
    public:
        TruncateQuery(PtrTo<ClusterClause> cluster, bool temporary, bool if_exists, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // TableIdentifier
        };

        const bool temporary, if_exists;
};

}
