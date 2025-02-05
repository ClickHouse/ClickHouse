#pragma once

#include <Parsers/IParserBase.h>

// cases
// - [123]
namespace DB
{
class ParserXPathIndexAccess : public IParserBase
{
private:
    const char * getName() const override { return "ParserXPathIndexAccess"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserXPathIndexAccess() = default;
};
}
