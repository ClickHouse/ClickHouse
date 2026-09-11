#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{

enum class TupleElementCodecSyntax : UInt8
{
    Disallow,
    AllowSet,
    AllowSetAndRemove,
};

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
public:
    explicit ParserDataType(TupleElementCodecSyntax tuple_element_codec_syntax_ = TupleElementCodecSyntax::Disallow)
        : tuple_element_codec_syntax(tuple_element_codec_syntax_) {}

protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    TupleElementCodecSyntax tuple_element_codec_syntax;
};

}
