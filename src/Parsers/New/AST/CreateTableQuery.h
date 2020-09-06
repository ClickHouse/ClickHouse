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

        enum class ClauseType
        {
            DESCRIPTION,
            TABLE,
            FUNCTION,
        };

        auto getType() const { return clause_type; }

    private:
        ClauseType clause_type;

        SchemaClause(ClauseType type, PtrList exprs);
};

class CreateTableQuery : public DDLQuery
{
    public:
        CreateTableQuery(
            bool attach,
            bool temporary,
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<SchemaClause> schema,
            PtrTo<EngineClause> engine,
            PtrTo<SelectUnionQuery> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            SCHEMA,
            ENGINE,
            SUBQUERY,
        };

        const bool attach, temporary, if_not_exists;
};

}
