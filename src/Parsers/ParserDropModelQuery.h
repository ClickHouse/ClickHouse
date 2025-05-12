#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * DROP MODEL model_name
  */

class ParserDropModelQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP MODEL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
