#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * DROP|DETACH|TRUNCATE TABLE [IF EXISTS] [db.]name
  *
  * Or:
  * DROP DATABASE [IF EXISTS] db
  */
class ParserDropQuery : public IParserBase
{
protected:
    const char * getName() const { return "DROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseDropQuery(Pos & pos, ASTPtr & node, Expected & expected);
    bool parseDetachQuery(Pos  & pos, ASTPtr & node, Expected & expected);
    bool parseTruncateQuery(Pos & pos, ASTPtr & node, Expected & expected);
};

}
