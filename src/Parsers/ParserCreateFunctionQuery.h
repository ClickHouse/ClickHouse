#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE FUNCTION test AS x -> x || '1'
class ParserCreateFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
