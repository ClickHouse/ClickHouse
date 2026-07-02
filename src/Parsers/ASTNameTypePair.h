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
    /// Optional DEFAULT expression, e.g. `Tuple(s String DEFAULT 'Hello')`.
    /// This only exists at the syntax level: it is pulled up to the column level in
    /// InterpreterCreateQuery, and an attempt to build an actual data type while it is set throws.
    ASTPtr default_expression;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "NameTypePair" + (delim + name); }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

