#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDropRewriteRuleQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "DROP RULE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;
};
}
