#pragma once

#include "IParserBase.h"

namespace DB
{
/// DROP TYPE type1
class ParserDropDataTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
