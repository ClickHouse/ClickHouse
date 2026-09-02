#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserCreateRewriteRuleQuery : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;

    const char * getName() const override { return "CREATE RULE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserCreateRewriteRuleQuery(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {}
};
}
