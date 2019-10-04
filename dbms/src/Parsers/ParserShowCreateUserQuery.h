#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW CREATE USER user
  */
class ParserShowCreateUserQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
