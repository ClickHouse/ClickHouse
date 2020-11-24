#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

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
        static PtrTo<AlterTableClause> createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createAttach(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from);
        static PtrTo<AlterTableClause> createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionClause> in);
        static PtrTo<AlterTableClause> createCodec(bool if_exists, PtrTo<Identifier> identifier, PtrTo<CodecExpr> codec);
        static PtrTo<AlterTableClause> createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> comment);
        static PtrTo<AlterTableClause> createDelete(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createDetach(PtrTo<PartitionClause> clause);
        static PtrTo<AlterTableClause> createDropColumn(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createDropPartition(PtrTo<PartitionClause> clause);
        static PtrTo<AlterTableClause> createModify(bool if_exists, PtrTo<TableElementExpr> element);
        static PtrTo<AlterTableClause> createRemove(bool if_exists, PtrTo<Identifier> identifier, TableColumnPropertyType type);
        static PtrTo<AlterTableClause> createRemoveTTL();
        static PtrTo<AlterTableClause> createRename(bool if_exists, PtrTo<Identifier> identifier, PtrTo<Identifier> to);
        static PtrTo<AlterTableClause> createOrderBy(PtrTo<ColumnExpr> expr);
        static PtrTo<AlterTableClause> createReplace(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from);
        static PtrTo<AlterTableClause> createTTL(PtrTo<TTLClause> clause);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ADD
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

            // RENAME
            TO = 1,      // Identifier

            // TTL
            CLAUSE = 0,  // TTLClause
        };

        enum class ClauseType
        {
            ADD,
            ATTACH,
            CLEAR,
            CODEC,
            COMMENT,
            DELETE,
            DETACH,
            DROP_COLUMN,
            DROP_PARTITION,
            MODIFY,
            ORDER_BY,
            REMOVE,
            REMOVE_TTL,
            RENAME,
            REPLACE,
            TTL,
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
