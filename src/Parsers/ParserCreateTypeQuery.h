#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateTypeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE or ATTACH TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
