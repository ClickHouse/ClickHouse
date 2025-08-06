#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserXPath : public IParserBase
{
private:
    const char * getName() const override { return "ParserXPath"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserXPath() = default;
};

}
