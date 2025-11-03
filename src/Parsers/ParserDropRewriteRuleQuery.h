#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDropRewriteRuleQuery : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;

    const char * getName() const override { return "DROP RULE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
