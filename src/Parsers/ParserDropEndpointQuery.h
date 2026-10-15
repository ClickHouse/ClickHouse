#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserDropEndpointQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP ENDPOINT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
