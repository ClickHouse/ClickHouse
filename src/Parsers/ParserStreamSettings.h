#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

class ParserStreamSettings : public IParserBase
{
public:
    const char * getName() const override { return "STREAM Settings"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
