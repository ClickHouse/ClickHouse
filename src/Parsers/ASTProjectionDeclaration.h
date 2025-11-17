#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTSetQuery;

/** name (subquery) [settings ...]
  */
class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query;
    ASTSetQuery * options = nullptr;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&query));
        f(reinterpret_cast<void **>(&options));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
