#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * DROP TABLE [IF EXISTS|EMPTY] [db.]name [PERMANENTLY]
  *
  * Or:
  * DETACH|TRUNCATE TABLE [IF EXISTS] [db.]name [PERMANENTLY]
  *
  * Or:
  * DROP DATABASE [IF EXISTS] db
  *
  * Or:
  * DROP DICTIONARY [IF EXISTS] [db.]name
  */
class ParserDropQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "DROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
