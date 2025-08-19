#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE FUNCTION test AS x -> x || '1'
class ParserCreateAggregateFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE AGGREGATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
