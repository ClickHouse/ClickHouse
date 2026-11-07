#pragma once
#include <Parsers/IParserBase.h>

#include <string_view>


namespace DB
{

/// Whether `name` is accepted by `ParserDataType` as the name of a data type. When parsing we accept quoted type names
/// (e.g. `UInt64`), but when formatting we print them unquoted (e.g. UInt64), so names that would not parse back
/// unquoted (`x.y`, `Null`, `8`, ...) are rejected.
bool isValidDataTypeName(std::string_view name);

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

