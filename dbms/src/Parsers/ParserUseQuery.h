#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Запрос USE db
  */
class ParserUseQuery : public IParserBase
{
protected:
    const char * getName() const { return "USE query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
