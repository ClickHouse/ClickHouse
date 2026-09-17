#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserShowCreateTypeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
