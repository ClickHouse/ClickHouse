#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * UPDATE [db.]name SET ... WHERE ...
  */
class ParserUpdateQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Update query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
