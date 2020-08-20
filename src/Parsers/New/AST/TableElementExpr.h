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

    private:
        PropertyType property_type;
};

class TableElementExpr : public INode
{
    public:
        static PtrTo<TableElementExpr> createColumn(
            PtrTo<Identifier> name, PtrTo<ColumnTypeExpr> type, PtrTo<TableColumnPropertyExpr> property, PtrTo<ColumnExpr> ttl);

    private:
        enum class ExprType
        {
            COLUMN,
            INDEX,
            CONSTRAINT,
        };
        enum ChildIndex: UInt8
        {
            // COLUMN
            NAME = 0,
            TYPE = 1,
            PROPERTY = 2,
            TTL = 3,
        };

        const ExprType expr_type;

        TableElementExpr(ExprType type, PtrList exprs);
};

}
