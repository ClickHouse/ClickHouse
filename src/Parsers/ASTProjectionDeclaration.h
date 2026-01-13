#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTFunction;
class ASTSetQuery;

class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query = nullptr;
    IAST * index = nullptr;
    ASTFunction * type = nullptr;
    ASTSetQuery * with_settings = nullptr;

    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&query));
        f(reinterpret_cast<void **>(&index));
        f(reinterpret_cast<void **>(&type));
        f(reinterpret_cast<void **>(&with_settings));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
