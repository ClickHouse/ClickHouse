#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserExplainTextActions final : public IParserBase
{
protected:
    const char * getName() const override { return "EXPLAIN TEXT action list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

bool parseExplainTextBareSourceAndActions(IParser::Pos & pos, ASTPtr & query, ASTPtr & actions, Expected & expected, const char * end, bool allow_settings_after_format_in_insert);
}
