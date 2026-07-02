#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix.
class ParserQueryWithOutput : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;
    bool allow_pipe_syntax;

    const char * getName() const override { return "Query with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQueryWithOutput(const char * end_, bool allow_settings_after_format_in_insert_ = false, bool allow_pipe_syntax_ = false)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
        , allow_pipe_syntax(allow_pipe_syntax_)
    {
    }
};

}
