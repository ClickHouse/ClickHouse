#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW [ROW] POLICIES [CURRENT] [ON [database.]table]
  */
class ParserShowRowPoliciesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW POLICIES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
