#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW CREATE USER user
  */
class ParserShowCreateAccessQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE USER or SHOW CREATE SETTINGS PROFILE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
