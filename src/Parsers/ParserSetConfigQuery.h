#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like
  * SET CONFIG [GLOBAL] name {NONE | = value}
  */
class ParserSetConfigQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SET CONFIG query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
