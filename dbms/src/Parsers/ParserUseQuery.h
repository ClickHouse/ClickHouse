#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query USE db
  */
class ParserUseQuery : public IParserBase
{
protected:
    const char * getName() const { return "USE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
