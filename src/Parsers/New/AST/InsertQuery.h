#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class DataClause : public INode
{
    public:
        enum class ClauseType
        {
            FORMAT,
            SELECT,
            VALUES,
        };

        static PtrTo<DataClause> createFormat(PtrTo<Identifier> identifier, size_t data_offset);
        static PtrTo<DataClause> createSelect(PtrTo<SelectUnionQuery> query);
        static PtrTo<DataClause> createValues(size_t data_offset);

        auto getType() const { return clause_type; }
        auto getOffset() const { return offset; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            FORMAT = 0,    // Identifier
            SUBQUERY = 0,  // SelectUnionQuery
        };

        ClauseType clause_type;
        size_t offset = 0;

        DataClause(ClauseType type, PtrList exprs);
};

class InsertQuery : public Query
{
    public:
        static PtrTo<InsertQuery> createFunction(PtrTo<TableFunctionExpr> function, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause);
        static PtrTo<InsertQuery> createTable(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause);

        bool hasData() const { return get<DataClause>(DATA)->getType() != DataClause::ClauseType::SELECT; }
        size_t getDataOffset() const { return get<DataClause>(DATA)->getOffset(); }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,  // TableIdentifier
            FUNCTION = 0,    // TableFunctionExpr
            COLUMNS = 1,     // ColumnNameList
            DATA = 2,        // DataClause
        };
        enum class QueryType
        {
            FUNCTION,
            TABLE,
        };

        QueryType query_type;

        InsertQuery(QueryType type, PtrList exprs);

        String dumpInfo() const override { return String("has_data=") + (hasData() ? "true" : "false"); }
};

}
