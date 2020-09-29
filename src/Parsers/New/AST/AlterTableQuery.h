#pragma once

#include <Parsers/New/AST/DDLQuery.h>
#include "Parsers/New/AST/fwd_decl.h"


namespace DB::AST
{

class AlterTableClause : public INode
{
    public:
        static PtrTo<AlterTableClause> createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createAttach(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> from);
        static PtrTo<AlterTableClause> createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionExprList> in);
        static PtrTo<AlterTableClause> createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> comment);
        static PtrTo<AlterTableClause> createDelete(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createDetach(PtrTo<PartitionExprList> list);
        static PtrTo<AlterTableClause> createDropColumn(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createDropPartition(PtrTo<PartitionExprList> list);
        static PtrTo<AlterTableClause> createModify(bool if_exists, PtrTo<TableElementExpr> element);
        static PtrTo<AlterTableClause> createOrderBy(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createReplace(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> from);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ADD
            ELEMENT = 0,  // TableElementExpr
            AFTER = 1,    // Identifier (optional)

            // ATTACH/REPLACE
            PARTITION = 0,  // PartitionExprList
            FROM = 1,       // TableIdentifier (optional)

            // CLEAR
            COLUMN = 0,  // Identifier
            IN = 1,      // PartitionExprList

            // COMMENT
            COMMENT = 1,  // StringLiteral

            // DELETE
            EXPR = 0,  // ColumnExpr
        };

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

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,    // TableIdentifier
            CLAUSES = 1,  // List<AlterTableClause>
        };
};

}
