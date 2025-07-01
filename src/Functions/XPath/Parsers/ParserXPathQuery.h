#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserXPathQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ParserXPathQuery"; }
    bool parseImpl(Pos & pos, ASTPtr & query, Expected & expected) override;
};
}
