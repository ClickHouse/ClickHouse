#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateViewQuery : public DDLQuery
{
    public:
        CreateViewQuery(
            PtrTo<ClusterClause> cluster,
            bool attach,
            bool replace,
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<TableSchemaClause> clause,
            PtrTo<SelectUnionQuery> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,      // TableIdentifier
            SCHEMA = 1,    // TableSchemaClause (optional)
            SUBQUERY = 2,  // SelectUnionQuery
        };

        const bool attach, replace, if_not_exists;
};

}
