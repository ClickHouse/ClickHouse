#pragma once

#include <Parsers/New/AST/DDLQuery.h>
#include "Parsers/New/AST/fwd_decl.h"


namespace DB::AST
{

class AlterTableClause : public INode
{
    public:
        static PtrTo<AlterTableClause> createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createAttach(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier);
        static PtrTo<AlterTableClause> createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionExprList> clause);
        static PtrTo<AlterTableClause> createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> literal);
        static PtrTo<AlterTableClause> createDelete(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createDetach(PtrTo<PartitionExprList> list);
        static PtrTo<AlterTableClause> createDropColumn(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createDropPartition(PtrTo<PartitionExprList> list);
        static PtrTo<AlterTableClause> createModify(bool if_exists, PtrTo<TableElementExpr> element);
        static PtrTo<AlterTableClause> createOrderBy(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createReplace(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier);

    private:
        enum class ClauseType
        {
            ADD,
            ATTACH,
            CLEAR,
            COMMENT,
            DELETE,
            DETACH,
            DROP_COLUMN,
            DROP_PARTITION,
            MODIFY,
            ORDER_BY,
            REPLACE,
        };

        const ClauseType clause_type;
        union
        {
            bool if_exists;
            bool if_not_exists;
        };

        AlterTableClause(ClauseType type, PtrList exprs);
};

class AlterTableQuery : public DDLQuery
{
    public:
        AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses);

        ASTPtr convertToOld() const override;
};

}
