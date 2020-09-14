#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class JsonValue : public INode
{
    public:
        JsonValue(PtrTo<StringLiteral> key, PtrTo<JsonExpr> value);
};

class JsonExpr : public INode
{
    public:
        static PtrTo<JsonExpr> createBoolean(bool value);
        static PtrTo<JsonExpr> createLiteral(PtrTo<Literal> literal);
        static PtrTo<JsonExpr> createObject(PtrTo<JsonValueList> list);

    private:
        enum class ExprType
        {
            BOOLEAN,
            LITERAL,
            OBJECT,
        };

        ExprType expr_type;
        bool value;

        JsonExpr(ExprType type, PtrList exprs);
};

}
