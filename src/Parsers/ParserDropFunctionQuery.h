#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// DROP FUNCTION function1
class ParserDropFunctionQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "DROP FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
