#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class DataClause : public INode
{
    public:
        static PtrTo<DataClause> createFormat(PtrTo<Identifier> identifier);
        static PtrTo<DataClause> createSelect(PtrTo<SelectUnionQuery> query);
        static PtrTo<DataClause> createValues(PtrTo<ColumnExprList> list);

    private:
        enum class ClauseType
        {
            FORMAT,
            SELECT,
            VALUES,
        };

        ClauseType clause_type;

        DataClause(ClauseType type, PtrList exprs);
};

class InsertQuery : public Query
{
    public:
        static PtrTo<InsertQuery> createFunction(PtrTo<TableFunctionExpr> function, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause);
        static PtrTo<InsertQuery> createTable(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause);

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
};

}
