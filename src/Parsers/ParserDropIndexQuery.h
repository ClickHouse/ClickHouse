#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * DROP INDEX [IF EXISTS] name ON [db].name
  */

class ParserDropIndexQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP INDEX query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
