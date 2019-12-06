#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * DROP QUOTA [IF EXISTS] name [,...]
  */
class ParserDropAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
