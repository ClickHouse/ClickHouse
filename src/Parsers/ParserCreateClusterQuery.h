#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateClusterQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
