#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserDropShardQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
