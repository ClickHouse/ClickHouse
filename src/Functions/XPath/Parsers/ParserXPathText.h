#pragma once

#include <Parsers/IParserBase.h>

// cases
// - /text()
namespace DB
{
class ParserXPathText : public IParserBase
{
private:
    const char * getName() const override { return "ParserXPathText"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserXPathText() = default;
};
}
