#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserShowGrantsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW GRANTS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
