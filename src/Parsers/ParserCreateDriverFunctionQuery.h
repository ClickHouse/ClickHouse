#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE FUNCTION my_function(arg1 Type1, arg2 Type2) RETURNS Type3
/// ENGINE = DriverName AS
/// $$ code is here... $$

class ParserCreateDriverFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DRIVER FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
