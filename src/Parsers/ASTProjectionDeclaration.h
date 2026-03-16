#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTSetQuery;

/** name (subquery) [WITH SETTINGS (...)]
  */
class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query;
    ASTSetQuery * with_settings = nullptr;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&query));
        f(reinterpret_cast<void **>(&with_settings));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
