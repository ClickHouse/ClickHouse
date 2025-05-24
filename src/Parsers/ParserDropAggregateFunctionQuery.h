#pragma once

#include "IParserBase.h"

namespace DB
{
/// DROP FUNCTION function1
class ParserDropAggregateFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP AGGREGATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
