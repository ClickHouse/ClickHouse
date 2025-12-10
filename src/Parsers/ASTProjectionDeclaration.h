#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTFunction;

class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query;
    IAST * index;
    ASTFunction * type;

    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&query));
        f(reinterpret_cast<void **>(&index));
        f(reinterpret_cast<void **>(&type));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
