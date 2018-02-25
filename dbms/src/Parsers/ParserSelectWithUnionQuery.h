#pragma once

#include <Parsers/ParserQueryWithOutput.h>


namespace DB
{


class ParserSelectWithUnionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query, possibly with UNION"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};

}
