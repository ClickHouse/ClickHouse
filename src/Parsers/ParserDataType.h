#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

