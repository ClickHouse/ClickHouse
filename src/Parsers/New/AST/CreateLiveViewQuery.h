#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateLiveViewQuery : public DDLQuery
{
    public:
        CreateLiveViewQuery(
            PtrTo<ClusterClause> cluster,
            bool attach,
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<UUIDClause> uuid,
            PtrTo<NumberLiteral> timeout,
            PtrTo<DestinationClause> destination,
            PtrTo<TableSchemaClause> schema,
            PtrTo<SelectUnionQuery> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,     // TableIdentifier
            UUID,         // UUIDClause (optional)
            TIMEOUT,      // NumberLiteral (optional)
            DESTINATION,  // DestinationClause (optional)
            SCHEMA,       // TableSchemaClause (optional)
            SUBQUERY,     // SelectUnionQuery
        };

        const bool attach, if_not_exists;
};

}
