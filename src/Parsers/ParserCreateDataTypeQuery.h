#pragma once

#include "IParserBase.h"

namespace DB
{
/// CREATE TYPE type1 AS UInt32
/// Or: CREATE TYPE type1 AS Tuple(Float64, Float64)
class ParserCreateDataTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
