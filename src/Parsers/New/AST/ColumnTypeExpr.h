#pragma once

#include <Parsers/New/AST/INode.h>

#include <list>


namespace DB::AST
{

class EnumValue : public INode
{
    public:
        EnumValue(PtrTo<StringLiteral> name, PtrTo<NumberLiteral> value);

        ASTPtr convertToOld() const override;
        String toString() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,   // StringLiteral
            VALUE = 1,  // NumberLiteral
        };
};

class ColumnTypeExpr : public INode
{
    public:
        static PtrTo<ColumnTypeExpr> createSimple(PtrTo<Identifier> identifier);
        static PtrTo<ColumnTypeExpr> createNamed(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExpr> type);
        static PtrTo<ColumnTypeExpr> createComplex(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExprList> list);
        static PtrTo<ColumnTypeExpr> createEnum(PtrTo<Identifier> identifier, PtrTo<EnumValueList> list);
        static PtrTo<ColumnTypeExpr> createParam(PtrTo<Identifier> identifier, PtrTo<ColumnParamList> list);
        static PtrTo<ColumnTypeExpr> createNested(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExprList> list);

        ASTPtr convertToOld() const override;
        String toString() const override;

    private:
        enum class ExprType
        {
            SIMPLE,
            NAMED,
            COMPLEX,
            ENUM,
            PARAM,
            NESTED,
        };
        enum ChildIndex : UInt8
        {
            NAME = 0,  // Identifier
            TYPE = 1,  // ColumnTypeExpr
            LIST = 1,  // depends on |expr_type|
        };

        ExprType expr_type;

        ColumnTypeExpr(ExprType type, PtrList exprs);
};

}
