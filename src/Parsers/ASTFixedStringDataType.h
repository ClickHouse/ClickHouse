#pragma once

#include <Parsers/ASTDataType.h>


namespace DB
{

/// Specialized AST for FixedString data type
/// Stores the length directly as a field instead of an ASTLiteral child.
class ASTFixedStringDataType : public ASTDataType
{
public:
    UInt64 n = 0;  /// Length of the fixed string

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    /// Outputs: FixedString(n)
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
