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
        InsertQuery(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<ValuesClause> clause);
};

}
