#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserAlterShardQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
