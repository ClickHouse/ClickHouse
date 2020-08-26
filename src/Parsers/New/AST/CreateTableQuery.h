#pragma once

#include <Parsers/New/AST/DDLQuery.h>
#include "Parsers/New/AST/SelectUnionQuery.h"


namespace DB::AST
{

class SchemaClause : public INode
{
    public:
        static PtrTo<SchemaClause> createDescription(PtrTo<TableElementList> list);
        static PtrTo<SchemaClause> createAsTable(PtrTo<TableIdentifier> identifier);
        static PtrTo<SchemaClause> createAsFunction(PtrTo<Identifier> identifier, PtrTo<TableArgList> list);

    private:
        enum class ClauseType
        {
            DESCRIPTION,
            TABLE,
            FUNCTION,
        };

        ClauseType clause_type;

        SchemaClause(ClauseType type, PtrList exprs);
};

class CreateTableQuery : public DDLQuery
{
    public:
        CreateTableQuery(
            bool temporary,
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

        const bool temporary, if_not_exists;
};

}
