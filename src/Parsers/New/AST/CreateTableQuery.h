#pragma once

#include <Parsers/New/AST/DDLQuery.h>
#include "Parsers/New/AST/SelectUnionQuery.h"


namespace DB::AST
{

class TableSchemaClause : public INode
{
    public:
        static PtrTo<TableSchemaClause> createDescription(PtrTo<TableElementList> list);
        static PtrTo<TableSchemaClause> createAsTable(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableSchemaClause> createAsFunction(PtrTo<TableFunctionExpr> expr);

        enum class ClauseType
        {
            DESCRIPTION,
            TABLE,
            FUNCTION,
        };

        auto getType() const { return clause_type; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // DESCRIPTION
            ELEMENTS = 0,  // TableElementList

            // TABLE and FUNCTION
            EXPR = 0,      // TableIdentifier or TableFunctionExpr
        };

        ClauseType clause_type;

        TableSchemaClause(ClauseType type, PtrList exprs);

        String dumpInfo() const override;
};

class CreateTableQuery : public DDLQuery
{
    public:
        CreateTableQuery(
            PtrTo<ClusterClause> cluster,
            bool attach,
            bool temporary,
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<UUIDClause> uuid,
            PtrTo<TableSchemaClause> schema,
            PtrTo<EngineClause> engine,
            PtrTo<SelectUnionQuery> query);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // TableIdentifier
            UUID,      // UUIDClause (optional)
            SCHEMA,    // TableSchemaClause
            ENGINE,    // EngineClause
            SUBQUERY,  // SelectUnionQuery
        };

        const bool attach, temporary, if_not_exists;

        String dumpInfo() const override;
};

}
