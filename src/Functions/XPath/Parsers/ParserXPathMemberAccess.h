#pragma once

#include <Parsers/IParserBase.h>

// cases
// - //member
// — /member
namespace DB
{
class ParserXPathMemberAccess : public IParserBase
{
private:
    const char * getName() const override { return "ParserXPathMemberAccess"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserXPathMemberAccess() = default;
};
}
