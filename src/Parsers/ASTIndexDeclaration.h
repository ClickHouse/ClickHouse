#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>


namespace DB
{

/** name BY expr TYPE typename(args) GRANULARITY int in create query
  */
class ASTIndexDeclaration : public IAST
{
public:
    String name;
    IAST * expr;
    ASTFunction * type;
    UInt64 granularity;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
