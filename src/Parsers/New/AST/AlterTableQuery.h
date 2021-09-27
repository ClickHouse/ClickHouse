#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AssignmentExpr : public INode
{
    public:
        AssignmentExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExpr> expr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,  // Identifier
            EXPR = 1,        // ColumnExpr
        };
};

enum class TableColumnPropertyType
{
    ALIAS,
    CODEC,
    COMMENT,
    DEFAULT,
    MATERIALIZED,
    TTL,
};

class PartitionClause : public INode
{
    public:
        explicit PartitionClause(PtrTo<Literal> id);
        explicit PartitionClause(PtrTo<List<Literal>> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            ID = 0,    // Literal
            LIST = 0,  // List<Literal>
        };
        enum class ClauseType
        {
            ID,
            LIST,
        };

        const ClauseType clause_type;

        PartitionClause(ClauseType type, PtrList exprs);
};

class AlterTableClause : public INode
{
    public:
        static PtrTo<AlterTableClause> createAddColumn(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createAddIndex(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createAttach(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from);
        static PtrTo<AlterTableClause> createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionClause> in);
        static PtrTo<AlterTableClause> createCodec(bool if_exists, PtrTo<Identifier> identifier, PtrTo<CodecExpr> codec);
        static PtrTo<AlterTableClause> createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> comment);
        static PtrTo<AlterTableClause> createDelete(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createDetach(PtrTo<PartitionClause> clause);
        static PtrTo<AlterTableClause> createDropColumn(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createDropIndex(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createDropPartition(PtrTo<PartitionClause> clause);
        static PtrTo<AlterTableClause> createFreezePartition(PtrTo<PartitionClause> clause);
        static PtrTo<AlterTableClause> createModify(bool if_exists, PtrTo<TableElementExpr> element);
        static PtrTo<AlterTableClause> createMovePartitionToDisk(PtrTo<PartitionClause> clause, PtrTo<StringLiteral> literal);
        static PtrTo<AlterTableClause> createMovePartitionToTable(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> identifier);
        static PtrTo<AlterTableClause> createMovePartitionToVolume(PtrTo<PartitionClause> clause, PtrTo<StringLiteral> literal);
        static PtrTo<AlterTableClause> createRemove(bool if_exists, PtrTo<Identifier> identifier, TableColumnPropertyType type);
        static PtrTo<AlterTableClause> createRemoveTTL();
        static PtrTo<AlterTableClause> createRename(bool if_exists, PtrTo<Identifier> identifier, PtrTo<Identifier> to);
        static PtrTo<AlterTableClause> createOrderBy(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createReplace(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from);
        static PtrTo<AlterTableClause> createTTL(PtrTo<TTLClause> clause);
        static PtrTo<AlterTableClause> createUpdate(PtrTo<AssignmentExprList> list, PtrTo<WhereClause> where);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ADD COLUMN or INDEX
            ELEMENT = 0,  // TableElementExpr
            AFTER = 1,    // Identifier (optional)

            // ATTACH/REPLACE
            PARTITION = 0,  // PartitionClause
            FROM = 1,       // TableIdentifier (optional)

            // CLEAR
            COLUMN = 0,  // Identifier
            IN = 1,      // PartitionClause

            // CODEC
            CODEC = 1,  // CodecExpr

            // COMMENT
            COMMENT = 1,  // StringLiteral

            // DELETE
            EXPR = 0,  // ColumnExpr

            // MOVE
            // TO = 1,  // TableIdentifier or StringLiteral

            // RENAME
            TO = 1,      // Identifier

            // TTL
            CLAUSE = 0,  // TTLClause

            // UPDATE
            ASSIGNMENTS = 0,  // AssignmentExprList
            WHERE = 1,        // WhereClause
        };

        enum class ClauseType
        {
            ADD_COLUMN,
            ADD_INDEX,
            ATTACH,
            CLEAR,
            CODEC,
            COMMENT,
            DELETE,
            DETACH,
            DROP_COLUMN,
            DROP_INDEX,
            DROP_PARTITION,
            FREEZE_PARTITION,
            MODIFY,
            MOVE_PARTITION_TO_DISK,
            MOVE_PARTITION_TO_TABLE,
            MOVE_PARTITION_TO_VOLUME,
            ORDER_BY,
            REMOVE,
            REMOVE_TTL,
            RENAME,
            REPLACE,
            TTL,
            UPDATE,
        };

        const ClauseType clause_type;
        TableColumnPropertyType property_type = TableColumnPropertyType::ALIAS;  // default value to silence PVS-Studio
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
        AlterTableQuery(PtrTo<ClusterClause> cluster, PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,    // TableIdentifier
            CLAUSES = 1,  // List<AlterTableClause>
        };
};

}
