#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE ROLE [IF NOT EXISTS] name [,...]
  */
class ParserCreateRoleQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE ROLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
