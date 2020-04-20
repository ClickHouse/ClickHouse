#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * DROP USER [IF EXISTS] name [,...]
  * DROP ROLE [IF EXISTS] name [,...]
  * DROP QUOTA [IF EXISTS] name [,...]
  * DROP [SETTINGS] PROFILE [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  */
class ParserDropAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP access entity query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
