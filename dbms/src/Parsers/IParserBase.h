#pragma once

#include <Parsers/IParser.h>


namespace DB
{

/** Base class for most parsers
  */
class IParserBase : public IParser
{
public:
    bool parse(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);

protected:
    virtual bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) = 0;
};

}
