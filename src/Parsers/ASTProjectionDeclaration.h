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

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&query, nullptr);
        f(&index, nullptr);
        f(reinterpret_cast<IAST **>(&type), nullptr);
        f(reinterpret_cast<IAST **>(&with_settings), nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
