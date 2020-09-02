#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class DestinationClause : public INode
{
    public:
        explicit DestinationClause(PtrTo<TableIdentifier> identifier);
};

class CreateMaterializedViewQuery : public DDLQuery
{
    public:
        CreateMaterializedViewQuery(
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<SchemaClause> schema,
            PtrTo<DestinationClause> destination,
            PtrTo<EngineClause> engine,
            PtrTo<SelectUnionQuery> query);

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            SCHEMA,
            DESTINATION,
            ENGINE,
            SUBQUERY,
        };

        const bool if_not_exists;
};

}
