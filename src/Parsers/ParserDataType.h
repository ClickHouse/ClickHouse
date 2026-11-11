#pragma once
#include <Parsers/IParserBase.h>

#include <string_view>


namespace DB
{

/// Whether `name` is accepted by `ParserDataType` as the name of a data type. When parsing we accept quoted type names
/// (e.g. `UInt64`), but when formatting we print them unquoted (e.g. UInt64), so names that would not parse back
/// unquoted (`x.y`, `Null`, `8`, ...) are rejected.
bool isValidDataTypeName(std::string_view name);

/// The name that `ParserDataType` gives to the data type spelled `name` when it is followed by arguments.
/// The composite types of the SQL standard are spelled case-insensitively (`ARRAY(INTEGER)`, `map(K, V)`),
/// so `ParserDataType` replaces any spelling of `Array` and `Map` with the canonical name. For other names
/// returns `name` unchanged.
std::string_view getCanonicalNameOfDataTypeWithArguments(std::string_view name);

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

