#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    String column;
    /// True when the user wrote `INTERPOLATE ("Col" AS ...)`. In `standard` mode the analyzer must
    /// keep the target column case-sensitive — without this flag the target identifier rebuilt by
    /// `QueryTreeBuilder` would lose the quote info and `"MyCol"` could match output column `mycol`.
    bool column_is_double_quoted = false;
    ASTPtr expr;

    String getID(char delim) const override
    {
        /// Include the quote flag so two AST forms that only differ by `"Col"` vs `Col` don't share an ID.
        return String("InterpolateElement") + delim + "(column " + column + (column_is_double_quoted ? "/q" : "") + ")";
    }

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
