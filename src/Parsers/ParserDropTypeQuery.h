#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserDropTypeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "DROP TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
