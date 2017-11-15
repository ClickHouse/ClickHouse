#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] suffix.
class ParserQueryWithOutput : public IParserBase
{
protected:
    const char * getName() const override { return "Query with output"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
