#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] suffix.
class ParserQueryWithOutput : public IParserBase
{
protected:
    const char * getName() const override { return "Query with output"; }

    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;

protected:
    ParserWhiteSpaceOrComments ws;
};

}
