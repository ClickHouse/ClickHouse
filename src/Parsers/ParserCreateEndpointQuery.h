#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateEndpointQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE ENDPOINT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
