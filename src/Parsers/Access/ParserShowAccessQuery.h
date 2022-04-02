#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query SHOW ACCESS
  */
class ParserShowAccessQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW ACCESS query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
