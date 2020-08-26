#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AttachQuery : public DDLQuery
{
    public:
        AttachQuery(
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<SchemaClause> schema,
            PtrTo<EngineClause> engine,
            PtrTo<SelectUnionQuery> query);

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            SCHEMA = 1,
            ENGINE = 2,
            SUBQUERY = 3,
        };

        const bool if_not_exists;
};

}
