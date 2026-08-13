#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserAlterEndpointQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER ENDPOINT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
