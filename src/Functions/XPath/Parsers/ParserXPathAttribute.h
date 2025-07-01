#pragma once

#include <Parsers/IParserBase.h>

// cases
// - [@attr = "value"]
// - [@attr = 'value']
// - [@attr = value]
// - [@attr]
namespace DB
{
class ParserXPathAttribute : public IParserBase
{
private:
    const char * getName() const override { return "ParserXPathAttribute"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserXPathAttribute() = default;
};
}
