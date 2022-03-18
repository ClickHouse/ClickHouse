#pragma once

#include "IParserBase.h"

namespace DB
{
/// DROP FUNCTION function1
// !! test with new funcs
class ParserDropFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
