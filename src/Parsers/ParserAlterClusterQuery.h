#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserAlterClusterQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
