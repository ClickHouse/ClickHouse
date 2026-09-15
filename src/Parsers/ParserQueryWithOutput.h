#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix.
class ParserQueryWithOutput : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;

    bool parse_output_options = true;

    const char * getName() const override { return "Query with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQueryWithOutput(const char * end_, bool allow_settings_after_format_in_insert_ = false, bool parse_output_options_ = true)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
        , parse_output_options(parse_output_options_)
    {}
};

}
