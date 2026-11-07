#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like:
  * DROP TYPE [IF EXISTS] type_name
  */
class ParserDropTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
