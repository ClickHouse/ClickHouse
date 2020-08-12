#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class EnumValue : public INode
{
    public:
        EnumValue(PtrTo<StringLiteral> name, PtrTo<NumberLiteral> value);

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            VALUE = 1,
        };
};

class ColumnTypeExpr : public INode
{
    public:
        static PtrTo<ColumnTypeExpr> createSimple(PtrTo<Identifier> identifier);
        static PtrTo<ColumnTypeExpr> createComplex(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExprList> list);
        static PtrTo<ColumnTypeExpr> createEnum(PtrTo<Identifier> identifier, PtrTo<EnumValueList> list);
        static PtrTo<ColumnTypeExpr> createParam(PtrTo<Identifier> identifier, PtrTo<ColumnParamList> list);

    private:
        enum class ExprType
        {
            SIMPLE,
            COMPLEX,
            ENUM,
            PARAM,
        };

        ExprType expr_type;

        ColumnTypeExpr(ExprType type, PtrList exprs);
};

}
