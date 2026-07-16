#pragma once

#include <Core/IdentifierName.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    String column;
    /// Quoting of the target column as written in the query. A double-quoted target
    /// stays case-sensitive under `standard` name matching.
    IdentifierPartQuote column_quote = IdentifierPartQuote::Unquoted;
    ASTPtr expr;

    /// `column_quote` stays out of the id: quote styles do not survive formatting (see the
    /// hashing policy note in `ASTIdentifier::updateTreeHashImpl`).
    String getID(char delim) const override { return String("InterpolateElement") + delim + "(column " + column + ")"; }

    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
