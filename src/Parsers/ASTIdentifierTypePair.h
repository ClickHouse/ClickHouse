#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier_fwd.h>


namespace DB
{

/// A pair of the identifier and type.
class ASTIdentifierTypePair : public IAST
{
public:
    ASTPtr identifier;
    ASTPtr type;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "NameTypePair" + (delim + identifier->getID()); }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

