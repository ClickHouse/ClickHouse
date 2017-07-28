#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * DROP|DETACH TABLE [IF EXISTS] [db.]name
  *
  * Or:
  * DROP DATABASE [IF EXISTS] db
  */
class ParserDropQuery : public IParserBase
{
protected:
    const char * getName() const { return "DROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
