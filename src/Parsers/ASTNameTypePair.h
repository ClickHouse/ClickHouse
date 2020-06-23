#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** A pair of the name and type. For example, browser FixedString(2).
  */
class ASTNameTypePair : public IAST
{
public:
    /// name
    String name;
    /// type
    ASTPtr type;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "NameTypePair" + (delim + name); }
    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

