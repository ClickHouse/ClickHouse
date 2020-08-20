#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class SchemaClause : public INode
{
    public:
        static PtrTo<SchemaClause> createDescription(PtrTo<TableElementList> list, PtrTo<EngineClause> clause);
        static PtrTo<SchemaClause> createAsSubquery(PtrTo<SelectUnionQuery> query, PtrTo<EngineClause> clause);
        static PtrTo<SchemaClause> createAsTable(PtrTo<TableIdentifier> identifier, PtrTo<EngineClause> clause);
        static PtrTo<SchemaClause> createAsFunction(PtrTo<Identifier> identifier, PtrTo<TableArgList> list);

    private:
        enum class ClauseType
        {
            DESCRIPTION,
            SUBQUERY,
            TABLE,
            FUNCTION,
        };

        ClauseType clause_type;

        SchemaClause(ClauseType type, PtrList exprs);
};

class CreateTableQuery : public DDLQuery
{
    public:
        CreateTableQuery(bool temporary, bool if_not_exists, PtrTo<TableIdentifier> identifier, PtrTo<SchemaClause> clause);

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            SCHEMA = 1,
        };

        const bool temporary, if_not_exists;
};

}
