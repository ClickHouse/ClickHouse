#pragma once

#include <Parsers/ASTDataType.h>
#include <vector>
#include <utility>


namespace DB
{

/// Specialized AST for Enum data types (Enum8, Enum16, Enum)
/// Stores enum values directly as vector of pairs instead of ASTLiteral children,
/// significantly reducing memory allocations for enums with many values.
class ASTEnumDataType : public ASTDataType
{
public:
    /// Direct storage of enum values: (name, value) pairs
    /// No ASTFunction/ASTLiteral children needed
    std::vector<std::pair<String, Int64>> values;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    /// `values` are stored directly (not as AST children), so the generic `ASTDataType` JSON
    /// serialization would drop them. Serialize/restore them explicitly under a distinct type tag.
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    /// Outputs: Enum8('name1' = 1, 'name2' = 2, ...)
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
