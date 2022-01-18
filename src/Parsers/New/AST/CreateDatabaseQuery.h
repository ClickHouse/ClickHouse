#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateDatabaseQuery: public DDLQuery
{
    public:
        CreateDatabaseQuery(PtrTo<ClusterClause> cluster, bool if_not_exists, PtrTo<DatabaseIdentifier> identifier, PtrTo<EngineExpr> expr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,    // DatabaseIdentifier
            ENGINE = 1,  // EngineExpr (optional)
        };

        const bool if_not_exists;
};

}
