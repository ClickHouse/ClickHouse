#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserDropClusterQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
