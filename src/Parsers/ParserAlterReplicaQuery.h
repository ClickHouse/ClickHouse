#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserAlterReplicaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER REPLICA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
