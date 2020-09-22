#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

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

    private:
        PropertyType property_type;
};

class TableElementExpr : public INode
{
    public:
        enum class ExprType
        {
            COLUMN,
            INDEX,
            CONSTRAINT,
        };

        static PtrTo<TableElementExpr> createColumn(
            PtrTo<Identifier> name,
            PtrTo<ColumnTypeExpr> type,
            PtrTo<TableColumnPropertyExpr> property,
            PtrTo<StringLiteral> comment,
            PtrTo<ColumnExpr> ttl);

        auto getType() const { return expr_type; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex: UInt8
        {
            // COLUMN
            NAME = 0,  // Identifier
            TYPE,      // ColumnExprType (optional)
            PROPERTY,  // TableColumnPropertyExpr
            COMMENT,   // StringLiteral
            TTL,       // ColumnExpr
        };

        const ExprType expr_type;

        TableElementExpr(ExprType type, PtrList exprs);
};

}
