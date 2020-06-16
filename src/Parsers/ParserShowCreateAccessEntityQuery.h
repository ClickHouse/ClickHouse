#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW CREATE QUOTA [name]
  */
class ParserShowCreateAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
