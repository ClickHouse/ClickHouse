#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class TableElementPropertyExpr : public INode
{
    public:
        enum class PropertyType
        {
            DEFAULT,
            MATERIALIZED,
            ALIAS,
        };

        TableElementPropertyExpr(PropertyType type, PtrTo<ColumnExpr> expr);

    private:
        PropertyType property_type;
};

class TableElementExpr : public INode
{
    public:
        static PtrTo<TableElementExpr> createColumn(
            PtrTo<Identifier> name, PtrTo<Identifier> type, PtrTo<TableElementPropertyExpr> property, PtrTo<ColumnExpr> ttl);

    private:
        enum class ExprType
        {
            COLUMN,
            INDEX,
            CONSTRAINT,
        };

        ExprType expr_type;

        TableElementExpr(ExprType type, PtrList exprs);
};

}
