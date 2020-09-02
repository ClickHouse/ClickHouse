#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AlterPartitionClause : public INode
{
    public:
        static PtrTo<AlterPartitionClause> createDetach(PtrTo<PartitionExprList> list);
        static PtrTo<AlterPartitionClause> createDrop(PtrTo<PartitionExprList> list);

    private:
        enum class ClauseType
        {
            DETACH,
            DROP,
        };

        ClauseType clause_type;

        AlterPartitionClause(ClauseType type, PtrList exprs);
};

class AlterPartitionQuery : public DDLQuery
{
    public:
        AlterPartitionQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterPartitionClause>> clauses);
};

}
