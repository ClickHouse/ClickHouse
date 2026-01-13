#pragma once

#include <Parsers/ASTDataType.h>


namespace DB
{

/// Specialized AST for Decimal data types (Decimal, Decimal32, Decimal64, Decimal128, Decimal256)
/// Stores precision and scale directly as fields instead of ASTLiteral children.
class ASTDecimalDataType : public ASTDataType
{
public:
    UInt32 precision = 0;
    UInt32 scale = 0;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    /// Outputs: Decimal(precision, scale) or Decimal32(scale) etc.
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
