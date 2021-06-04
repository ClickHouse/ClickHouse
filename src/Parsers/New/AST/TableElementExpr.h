#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class CodecArgExpr : public INode
{
    public:
        CodecArgExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // Identifier
            ARGS = 1,  // ColumnExprList (optional)
        };
};

class CodecExpr : public INode
{
    public:
        explicit CodecExpr(PtrTo<CodecArgList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            ARGS = 0,  // CodecArgList
        };
};

class TableColumnPropertyExpr : public INode
{
    public:
        enum class PropertyType
        {
            DEFAULT,
            MATERIALIZED,
            ALIAS,
        };

        TableColumnPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr);

        auto getType() const { return property_type; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // ColumnExpr
        };

        PropertyType property_type;
};

class TableElementExpr : public INode
{
    public:
        enum class ExprType
        {
            COLUMN,
            CONSTRAINT,
            INDEX,
        };

        static PtrTo<TableElementExpr> createColumn(
            PtrTo<Identifier> name,
            PtrTo<ColumnTypeExpr> type,
            PtrTo<TableColumnPropertyExpr> property,
            PtrTo<StringLiteral> comment,
            PtrTo<CodecExpr> codec,
            PtrTo<ColumnExpr> ttl);

        static PtrTo<TableElementExpr> createConstraint(PtrTo<Identifier> identifier, PtrTo<ColumnExpr> expr);

        static PtrTo<TableElementExpr>
        createIndex(PtrTo<Identifier> name, PtrTo<ColumnExpr> expr, PtrTo<ColumnTypeExpr> type, PtrTo<NumberLiteral> granularity);

        auto getType() const { return expr_type; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex: UInt8
        {
            // COLUMN
            NAME = 0,      // Identifier
            TYPE = 1,      // ColumnExprType (optional)
            PROPERTY = 2,  // TableColumnPropertyExpr
            COMMENT = 3,   // StringLiteral (optional)
            CODEC = 4,     // CodecExpr (optional)
            TTL = 5,       // ColumnExpr (optional)

            // CONSTRAINT
            // NAME = 0,
            // EXPR = 1,

            // INDEX
            EXPR = 1,         // ColumnExpr
            INDEX_TYPE = 2,   // ColumnTypeExpr
            GRANULARITY = 3,  // NumberLiteral
        };

        const ExprType expr_type;

        TableElementExpr(ExprType type, PtrList exprs);
};

}
