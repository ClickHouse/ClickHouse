#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>


namespace DB
{
/** name (subquery)
  */
class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
