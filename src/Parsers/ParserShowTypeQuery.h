#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like
  * SHOW TYPE type_name [FORMAT format] [INTO OUTFILE filename]
  */
class ParserShowTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
