#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserDescribeTypeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "DESCRIBE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
