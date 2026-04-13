#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDropReplicaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP REPLICA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
