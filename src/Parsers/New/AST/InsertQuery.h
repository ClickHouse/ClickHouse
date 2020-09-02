#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ValuesClause : public INode
{
    public:
        static PtrTo<ValuesClause> createValues(PtrTo<ColumnExprList> list);
        static PtrTo<ValuesClause> createSelect(PtrTo<SelectUnionQuery> query);

    private:
        enum class ClauseType
        {
            VALUES,
            SELECT,
        };

        ClauseType clause_type;

        ValuesClause(ClauseType type, PtrList exprs);
};

class InsertQuery : public Query
{
    public:
        static PtrTo<InsertQuery> createTable(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<ValuesClause> clause);
        static PtrTo<InsertQuery> createFunction(PtrTo<Identifier> name, PtrTo<TableArgList> args, PtrTo<ValuesClause> clause);

    private:
        enum class QueryType
        {
            TABLE,
            FUNCTION,
        };

        QueryType query_type;

        InsertQuery(QueryType type, PtrList exprs);
};

}
