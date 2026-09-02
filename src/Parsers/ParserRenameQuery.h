#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * RENAME TABLE [db.]name TO [db.]name, [db.]name TO [db.]name, ...
  * (An arbitrary number of tables can be renamed.)
  */
class ParserRenameQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "RENAME query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
