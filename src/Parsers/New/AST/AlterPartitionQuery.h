#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AlterPartitionClause : public INode
{
    public:
        static PtrTo<AlterPartitionClause> createAttach(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier);
        static PtrTo<AlterPartitionClause> createDetach(PtrTo<PartitionExprList> list);
        static PtrTo<AlterPartitionClause> createDrop(PtrTo<PartitionExprList> list);
        static PtrTo<AlterPartitionClause> createReplace(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier);

    private:
        enum class ClauseType
        {
            ATTACH,
            DETACH,
            DROP,
            REPLACE,
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
