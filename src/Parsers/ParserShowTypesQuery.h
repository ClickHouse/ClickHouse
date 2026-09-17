#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserShowTypesQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW TYPES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
