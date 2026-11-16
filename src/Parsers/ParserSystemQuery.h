#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSystemQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SYSTEM query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
