#pragma once

#include "IParserBase.h"

namespace DB
{
/// DROP RESOURCE resource1
class ParserDropResourceQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP RESOURCE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
