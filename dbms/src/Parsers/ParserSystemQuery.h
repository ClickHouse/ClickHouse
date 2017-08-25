#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSystemQuery : public IParserBase
{
public:
    ParserSystemQuery() = default;

protected:
    const char * getName() const override { return "SYSTEM query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
