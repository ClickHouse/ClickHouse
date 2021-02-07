#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateMaterializedViewQuery : public DDLQuery
{
    public:
        CreateMaterializedViewQuery(
            PtrTo<ClusterClause> cluster,
            bool attach,
            bool if_not_exists,
            bool populate,
            PtrTo<TableIdentifier> identifier,
            PtrTo<UUIDClause> uuid,
            PtrTo<TableSchemaClause> schema,
            PtrTo<DestinationClause> destination,
            PtrTo<EngineClause> engine,
            PtrTo<SelectUnionQuery> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,     // TableIdentifier
            UUID,         // UUIDClause (optional)
            SCHEMA,       // TableSchemaClause (optional)
            DESTINATION,  // DestinationClause (optional)
            ENGINE,       // EngineClause (optional)
            SUBQUERY,     // SelectUnionQuery
        };

        const bool attach, if_not_exists, populate;
};

}
