#pragma once

#include <Parsers/ASTDataType.h>


namespace DB
{

/// Specialized AST for DateTime64 data type
/// Stores precision and timezone directly as fields instead of ASTLiteral children.
class ASTDateTime64DataType : public ASTDataType
{
public:
    UInt32 precision = 3;  /// Default precision is 3 (milliseconds)
    String timezone;       /// Optional timezone

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    /// Outputs: DateTime64(precision) or DateTime64(precision, 'timezone')
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
