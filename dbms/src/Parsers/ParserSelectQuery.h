#pragma once

#include <Parsers/ParserQueryWithOutput.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};

}
