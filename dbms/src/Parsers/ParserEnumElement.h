#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

class ParserEnumElement : public IParserBase
{
    ParserStringLiteral name_parser;
    ParserNumber value_parser;

protected:
    const char * getName() const override { return "enum element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


}
