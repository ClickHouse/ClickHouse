#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
public:
    explicit ParserDataType(bool allow_tuple_element_codecs_ = false, bool allow_tuple_element_codec_removals_ = false)
        : allow_tuple_element_codecs(allow_tuple_element_codecs_)
        , allow_tuple_element_codec_removals(allow_tuple_element_codec_removals_)
    {
    }

protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_tuple_element_codecs;
    bool allow_tuple_element_codec_removals;
};

}
