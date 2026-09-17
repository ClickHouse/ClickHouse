#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserRenameTypeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
