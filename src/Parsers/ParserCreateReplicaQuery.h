#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateReplicaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE REPLICA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
