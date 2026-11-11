#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like
  * SHOW TYPES [FORMAT format] [INTO OUTFILE filename]
  */
class ParserShowTypesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW TYPES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
