#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

class ASTFunction;

/** name BY columns TYPE typename(args) in create query
  */
class ASTStatisticDeclaration : public IAST
{
public:
    String name;
    ASTExpressionList * columns;
    ASTFunction * type; // TODO: optional or 'AUTO'

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Stat"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
