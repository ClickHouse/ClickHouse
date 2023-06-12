#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserPRQLQuery : public IParserBase
{
public:
    const char * getName() const override { return "PRQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
