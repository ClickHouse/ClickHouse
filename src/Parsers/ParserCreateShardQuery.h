#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateShardQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
