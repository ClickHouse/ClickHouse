#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE TOKEN
  *     [{VALID UNTIL datetime | VALID FOR interval}]
  *     [GRANTS (privilege ON object [,...])]
  */
class ParserCreateTokenQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TOKEN query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
