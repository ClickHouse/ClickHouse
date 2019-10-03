#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Parses query like this:
  * DROP ROLE [IF EXISTS] role [,...]
  * DROP USER [IF EXISTS] user [,...]
  */
class ParserDropAccessQuery : public IParserBase
{
protected:
    const char * getName() const { return "DROP ROLE or DROP USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};
}
